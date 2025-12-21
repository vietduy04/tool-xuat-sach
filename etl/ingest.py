import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Union

import polars as pl

MAX_WORKERS = 6
SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
DATE_PATTERN = re.compile(r"(\d{4}_\d{2}_\d{2})__\d+")


@dataclass(frozen=True)
class ImportResult:
    df: pl.DataFrame
    rows: int
    date: str


FileInput = Union[str, Path, object]


# ---- helpers -------------------------------------------------
def file_name(file: FileInput) -> str:
    return file.name if hasattr(file, "name") else Path(file).name  # pyright: ignore[reportArgumentType, reportAttributeAccessIssue]


def file_ext(file: FileInput) -> str:
    return Path(file_name(file)).suffix.lower()


def extract_date(files: Iterable[FileInput]) -> str:
    dates = set()

    for file in files:
        match = DATE_PATTERN.search(file_name(file))
        if not match:
            return ""
        dates.add(match.group(1))

    if len(dates) != 1:
        return ""

    try:
        date = datetime.strptime(dates.pop(), "%Y_%m_%d")
        return date.strftime("%d-%m-%Y")
    except ValueError:
        return ""


# ---- loaders -------------------------------------------------
def load_threaded(
    files: Iterable[FileInput],
    reader,
    max_workers: int = MAX_WORKERS,
    **opts,
) -> pl.DataFrame:
    dfs: list[pl.DataFrame] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(reader, f, **opts): f for f in files}

        for future in as_completed(futures):
            file = futures[future]
            try:
                dfs.append(future.result())
            except Exception as exc:
                raise RuntimeError(f"Failed to read file: {file_name(file)}") from exc

    return pl.concat(dfs, rechunk=True)


def load_files(
    files: Iterable[FileInput],
    reader,
    use_threads: bool,
    **opts,
) -> pl.DataFrame:
    if use_threads:
        return load_threaded(files, reader, **opts)

    dfs = [reader(f, **opts) for f in files]
    return pl.concat(dfs, rechunk=True)


# ---- public API ----------------------------------------------
def import_files(
    files: Iterable[FileInput],
    fast_mode: bool = False,
) -> ImportResult:
    files = list(files)
    extensions = {file_ext(f) for f in files}

    if len(extensions) != 1:
        raise ValueError("Mixed or unsupported file extensions detected.")

    ext = extensions.pop()
    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError("Unsupported file extension detected.")

    date = extract_date(files)

    if ext == ".csv":
        df = load_files(
            files,
            reader=pl.read_csv,
            use_threads=fast_mode,
        )
    else:  # .xlsx
        df = load_files(
            files,
            reader=pl.read_excel,
            use_threads=fast_mode,
            read_options={"skip_rows": 1},
        )

    return ImportResult(
        df=df,
        rows=df.height,
        date=date,
    )
