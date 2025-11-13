"""Ingestion module: Excel to CSV conversion and column selection."""
import tempfile
import pandas as pd
from utils.logger import get_logger
import config

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
        df = pd.read_excel(excel_path, engine='calamine', skiprows=2, header=None)
        df.columns = config.RAW_COLUMNS
        df = df[config.COLS_TO_KEEP]
        
        temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        df.to_csv(temp_csv, index=False, header=False)
        logger.info(f"Converted to CSV: {temp_csv}")
        return temp_csv
    except Exception as e:
        logger.error(f"Error ingesting file {excel_path}: {e}")
        raise

