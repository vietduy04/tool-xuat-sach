"""Ingestion module"""

import os
import re
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Tuple, Union

import polars as pl
from utils.logger import get_logger

logger = get_logger()

## Global
MAX_WORKERS = 8
PERSISTENCE_STORE = os.path.join(os.getcwd(), ".store")


def load_excel(path: str, **read_options) -> pl.DataFrame:
    return pl.read_excel(path, **read_options)


def load_excel_with_threads(excel_files: list, **read_options) -> pl.DataFrame:
    dfs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(load_excel, f, **read_options) for f in excel_files]
        for future in as_completed(futures):
            dfs.append(future.result())
    return pl.concat(dfs)


def ingest_files(
    files: list, format: str, *, fast_mode: bool = False, **read_options
) -> pl.LazyFrame:
    if format == "csv":
        return pl.scan_csv(files)

    if format in ["xlsx", "xls"]:
        if fast_mode:
            return load_excel_with_threads(files, **read_options).lazy()
        else:
            dfs = [
                pl.read_excel(
                    f,
                )
                for f in files
            ]
            return pl.concat(dfs).lazy()
    raise Exception("Format không được hỗ trợ. Chỉ đọc được file csv, xls, xlsx.")


def extract_date_from_filenames(filenames) -> str:
    """
    Extracts the date from a list of filenames.
    Expected pattern contains YYYY_MM_DD before '__<index>'.
    If all files share the same date, returns it as 'dd-mm-yyyy'.
    Otherwise, returns an empty string.
    """
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



def ingest_raw(
    raw: Union[str, List], format: str, raw_folder: str
) -> Tuple[pl.DataFrame, int, str]:
    """
    Ingest raw data from zip, csv, or xlsx sources.

    Parameters
    ----------
    raw : str | list
        - zip: zip file name
        - csv/xlsx: list of Streamlit uploaded files
    format : str
        One of: 'zip', 'csv', 'xlsx'
    raw_folder : str
        Folder containing the zip file

    Returns
    -------
    ingested_df : pl.DataFrame
    n_rows : int
    report_date : str  (dd-mm-yyyy) or "" if invalid
    """
    dfs = []
    filenames = []

    if format == "zip":
        zip_path = os.path.join(raw_folder, raw)

        with zipfile.ZipFile(zip_path, "r") as z:
            filenames = z.namelist()
            report_date = extract_date_from_filenames(filenames)

            if not report_date:
                return pl.DataFrame(), 0, ""

            for name in filenames:
                with z.open(name) as f:
                    if name.lower().endswith(".csv"):
                        dfs.append(pl.read_csv(f))
                    elif name.lower().endswith(".xlsx"):
                        dfs.append(pl.read_excel(f))

    elif format == "csv":
        filenames = [f.name for f in raw]
        report_date = extract_date_from_filenames(filenames)

        if not report_date:
            return pl.DataFrame(), 0, ""

        for f in raw:
            dfs.append(pl.read_csv(f))

    elif format == "xlsx":
        filenames = [f.name for f in raw]
        report_date = extract_date_from_filenames(filenames)

        if not report_date:
            return pl.DataFrame(), 0, ""

        dfs = load_with_threads(raw)

    else:
        raise ValueError("Unsupported format. Use 'zip', 'csv', or 'xlsx'.")

    if not dfs:
        return pl.DataFrame(), 0, ""

    ingested_df = pl.concat(dfs, how="vertical")
    return ingested_df, ingested_df.height, report_date

def ingest_zip()