"""Output module: CSV writing and file combination."""
import os
import pandas as pd
from typing import List, Optional
from utils.logger import get_logger

logger = get_logger()


def write_chunk_to_csv(
    df: pd.DataFrame,
    rd_output_file: str,
    kn_output_file: str,
    write_header: bool = False
) -> None:
    """
    Write chunk to separate RD and KN CSV files.
    
    Args:
        df: DataFrame to write
        rd_output_file: Path to RD output file
        kn_output_file: Path to KN output file
        write_header: Whether to write header row
    """
    try:
        df_RD = df[df['phan_loai'] == 'RD']
        df_KN = df[df['phan_loai'] == 'KN']
        
        if not df_RD.empty:
            df_RD.to_csv(
                rd_output_file,
                mode='a',
                header=write_header,
                index=False,
                encoding='utf-8'
            )
        
        if not df_KN.empty:
            df_KN.to_csv(
                kn_output_file,
                mode='a',
                header=write_header,
                index=False,
                encoding='utf-8'
            )
        
        logger.debug(f"Written chunk: {len(df_RD)} RD rows, {len(df_KN)} KN rows")
    except Exception as e:
        logger.error(f"Error writing chunk to CSV: {e}")
        raise


def combine_csv_files(
    rd_file: str,
    kn_file: str,
    output_file: str
) -> None:
    """
    Combine RD and KN CSV files into a single output file.
    
    Args:
        rd_file: Path to RD CSV file
        kn_file: Path to KN CSV file
        output_file: Path to combined output file
    """
    try:
        logger.info(f"Combining CSV files into {output_file}")
        dfs = []
        
        if os.path.exists(rd_file) and os.path.getsize(rd_file) > 0:
            df_rd = pd.read_csv(rd_file, encoding='utf-8')
            dfs.append(df_rd)
            logger.info(f"Loaded {len(df_rd)} rows from RD file")
        
        if os.path.exists(kn_file) and os.path.getsize(kn_file) > 0:
            df_kn = pd.read_csv(kn_file, encoding='utf-8')
            dfs.append(df_kn)
            logger.info(f"Loaded {len(df_kn)} rows from KN file")
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"Combined file created with {len(combined_df)} total rows")
        else:
            logger.warning("No data to combine")
    except Exception as e:
        logger.error(f"Error combining CSV files: {e}")
        raise

