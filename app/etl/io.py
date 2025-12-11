"""Ingestion module"""

import os
import tempfile

import polars as pl
from utils.logger import get_logger

logger = get_logger()


def save_file(file, persistent: bool = False) -> str:
    """Save file to temporary or persistent location."""
    if persistent:
        # Save to a persistent config directory
        config_dir = os.path.join(os.getcwd(), ".store")
        os.makedirs(config_dir, exist_ok=True)
        file_path = os.path.join(config_dir, f"{file.name}")
    else:
        # Save to temporary location
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, file.name)

    with open(file_path, "wb") as f:
        f.write(file.getbuffer())
    return file_path


def ingest_excel(raw_files: list, tmp_dir: str) -> list[str]:
    outs = []
    for f in raw_files:
        ## Đọc file: skip 1 dòng đầu sau header do excel từ NOC có 2 dòng header
        df_tmp = pl.read_excel(f, columns=RAW_COLUMNS, read_options={"skip_rows": 1})
        base = os.path.splitext(os.path.basename(p))[0]
        out_path = os.path.join(tmp_dir, f"{base}.csv")
        df_tmp.write_csv(out_path)
        outs.append(out_path)
    return outs


def write_csv_to_excel(csv_file, path):
    """Write csv file to Excel."""
    try:
        logger.info("Ingesting CSV file")
        df = pl.read_csv(csv_file)
        df.write_excel(path)
    except Exception as e:
        logger.error(f"Error ingesting CSV file: {e}")
        raise
