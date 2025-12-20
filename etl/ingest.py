"""Import list of csv / xslx"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Tuple

import polars as pl

MAX_WORKERS = 4


def get_ext(file):
    name = file.name if hasattr(file, "name") else str(file)
    return Path(name).suffix.lower()


def extract_date(filenames) -> str:
    date_pattern = re.compile(r"(\d{4}_\d{2}_\d{2})__\d+")
    dates = set()

    for name in filenames:
        match = date_pattern.search(name)
        if not match:
            return ""
        dates.add(match.group(1))

    if len(dates) != 1:
        return ""

    try:
        date_obj = datetime.strptime(dates.pop(), "%Y_%m_%d")
        return date_obj.strftime("%d-%m-%Y")
    except ValueError:
        return ""


def load_excel(path: str, **read_options) -> pl.DataFrame:
    return pl.read_excel(path, **read_options)


def load_with_threads(
    excel_files: list, fast_mode: bool = False, **read_options
) -> pl.DataFrame:
    dfs = []
    if fast_mode:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(load_excel, f, **read_options) for f in excel_files
            ]
            for future in as_completed(futures):
                dfs.append(future.result())
    else:
        dfs = [pl.read_excel(f, **read_options) for f in excel_files]

    return pl.concat(dfs)


def import_files(files: list, fast_mode: bool = False) -> Tuple[pl.DataFrame, int, str]:
    extensions = {get_ext(f) for f in files}
    date = extract_date([f.name for f in files])

    if extensions == {".csv"}:
        df = pl.concat([pl.read_csv(f) for f in files])
        return df, df.height, date
    elif extensions == {".xlsx"}:
        df = load_with_threads(
            files, fast_mode=fast_mode, read_options={"skip_rows": 1}
        )
        return df, df.height, date
    else:
        raise ValueError("Mixed or unsupported file extensions detected.")
