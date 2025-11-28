"""Preprocessing module: Handle nulls and standardize datetime."""

from typing import List

import pandas as pd

from utils.logger import get_logger

logger = get_logger()


def preprocess_rules(
    rule_df: pd.DataFrame,
) -> pd.DataFrame:  ## Change to import (and validate rule)
    """
    Convert rule time columns to seconds and ngay_xuat to timedelta.

    Args:
        rule_df: DataFrame containing rules

    Returns:
        Preprocessed rules DataFrame
    """
    try:
        rule_df = rule_df.copy()
        # Ensure time columns are parsed as datetime.time
        for tcol in ["thoigian_nhapdau", "thoigian_nhapcuoi", "thoigian_xuat"]:
            if tcol in rule_df.columns:
                rule_df[tcol] = pd.to_datetime(rule_df[tcol], format="%H:%M:%S").dt.time

        rule_df["thoigian_nhapdau_s"] = rule_df["thoigian_nhapdau"].apply(
            lambda t: t.hour * 3600 + t.minute * 60 + t.second
        )
        rule_df["thoigian_nhapcuoi_s"] = rule_df["thoigian_nhapcuoi"].apply(
            lambda t: t.hour * 3600 + t.minute * 60 + t.second
        )
        rule_df["thoigian_xuat_s"] = rule_df["thoigian_xuat"].apply(
            lambda t: t.hour * 3600 + t.minute * 60 + t.second
        )

        # Convert ngay_xuat to timedelta (days)
        if "ngay_xuat" in rule_df.columns:
            rule_df["ngay_xuat_td"] = (
                rule_df["ngay_xuat"].astype(int).apply(lambda x: timedelta(days=int(x)))
            )

        return rule_df
    except Exception as e:
        logger.error(f"Error preprocessing rules: {e}")
        raise


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
        datetime_cols = ["tg_nhap_buucuc", "tg_laixe_nhan"]
        for col in datetime_cols:
            if col in df.columns:
                # Standardize datetime format
                df[col] = pd.to_datetime(
                    df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S"
                )

        logger.debug(f"Preprocessed chunk with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error preprocessing chunk: {e}")
        raise
