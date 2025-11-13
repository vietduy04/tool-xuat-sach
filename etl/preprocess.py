"""Preprocessing module: Handle nulls and standardize datetime."""
import pandas as pd
from typing import List
from utils.logger import get_logger

logger = get_logger()


def preprocess_chunk(df: pd.DataFrame, cols_to_keep: List[str]) -> pd.DataFrame:
    """
    Preprocess a chunk of data: assign columns, handle nulls and standardize datetime.
    
    Args:
        df: DataFrame chunk to preprocess (without column names)
        cols_to_keep: List of column names to assign
        
    Returns:
        Preprocessed DataFrame with assigned columns
    """
    try:
        df = df.copy()
        
        # Assign columns first (chunks from CSV don't have headers)
        df.columns = cols_to_keep
        
        # Handle nulls in datetime columns - convert to NaT if needed
        datetime_cols = ['tg_nhap_buucuc', 'tg_laixe_nhan']
        for col in datetime_cols:
            if col in df.columns:
                # Standardize datetime format
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        
        logger.debug(f"Preprocessed chunk with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error preprocessing chunk: {e}")
        raise

