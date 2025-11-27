"""Ingestion module: Excel to CSV conversion and column selection."""

import os
import tempfile

import pandas as pd
import streamlit as st

import config
from utils.logger import get_logger

logger = get_logger()


def convert_excel_to_csv(excel_path: str) -> str:
    """
    Convert Excel file to temporary CSV, selecting only required columns.

    Args:
        excel_path: Path to the Excel file

    Returns:
        Path to the temporary CSV file
    """
    try:
        logger.info(f"Ingesting Excel file: {excel_path}")
        df = pd.read_excel(excel_path, engine="calamine", skiprows=2, header=None)
        df.columns = config.RAW_COLUMNS
        df = df[config.COLS_TO_KEEP]

        temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        df.to_csv(temp_csv, index=False, header=False)
        logger.info(f"Converted to CSV: {temp_csv}")
        return temp_csv
    except Exception as e:
        logger.error(f"Error ingesting file {excel_path}: {e}")
        raise


@st.cache_data
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
