"""Configuration constants for the ETL pipeline."""

from typing import List

import polars as pl

# Raw columns from Excel file
RAW_COLUMNS: List[str] = [
    "ma_phieugui",
    "ma_tai",
    "don_hoan",
    "loai_hang",
    "trong_luong",
    "ma_buucuc_goc",
    "ma_buucuc_phat",
    "chi_nhanh_goc",
    "chi_nhanh_phat",
    "don_vi_khaithac",
    "dvc",
    "ma_dv_viettel",
    "loai_dv",
    "type",
    "id_ht_tuyenxe",
    "trip_picked_code",
    "hanh_trinh",
    "time_expect",
    "trip_code_expect",
    "trip_name_expect",
    "trip_id_expect",
    "thoi_gian_expect",
    "tg_nhap_buucuc",
    "tg_laixe_nhan",
    "arrival_time_reality",
    "leave_time_reality",
    "kpi_xuat_sach",
    "chinhanh_dvkt",
    "mien_dvkt",
    "phanloai_hang",
    "bien_kiemsoat",
    "chuyen_tuyen",
    "buucuc_xahoi",
    "buucuc_1ca",
    "deadline_htdt",
    "dg_htdt",
    "deadline_cldv",
    "dg_cldv",
]

# Columns to keep after ingestion
COLS_TO_KEEP: List[str] = [
    "ma_phieugui",
    "ma_tai",
    "don_hoan",
    "loai_hang",
    "don_vi_khaithac",
    "chi_nhanh_goc",
    "chi_nhanh_phat",
    "ma_buucuc_goc",
    "ma_buucuc_phat",
    "trong_luong",
    "loai_dv",
    "hanh_trinh",
    "tg_nhap_buucuc",
    "tg_laixe_nhan",
]

DTYPE_MAPPING = {
    "Int8": pl.Int8(),
    "Int16": pl.Int16(),
    "Int32": pl.Int32(),
    "Int64": pl.Int64(),
    "Date": pl.Date(),
    "Datetime": pl.Datetime(),
    "Duration": pl.Duration(),
    "Time": pl.Time(),
    "String": pl.String(),
    "Categorical": pl.Categorical(),
    "Boolean": pl.Boolean(),
    "Object": pl.Object(),
}

# Default configuration values
DEFAULT_CHUNK_SIZE: int = 100000
DEFAULT_OUTPUT_FOLDER: str = "output"
CONFIG_FILE: str = ".streamlit_config.json"
