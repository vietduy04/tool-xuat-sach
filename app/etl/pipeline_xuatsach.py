"""Pipeline Xuất sạch kho vùng tỉnh (HUB) & TTKT"""

import os
import tempfile
import time
from typing import Mapping, Dict

import polars as pl
import streamlit as st
from utils.logger import get_logger

from app.etl.io import ingest_excel

logger = get_logger()

# ----- CONFIG -----
CONFIG_FILE: str = "config_xuatsach.json"
DEFAULT_OUTPUT_FOLDER: str = "output"
DEFAULT_OUTPUT_FORMAT: str = "csv"

LOOKUP_OVERRIDES = {
    # don_vi_khai_thac: chi_nhanh_HUB
    "HUBTAN": "BDG",
    "HUBBHD": "BDH",
}

## ----- SCHEMA -----
RAW_COLUMNS: list[str] = [
    "ma_phieugui", "ma_tai", "don_hoan", "loai_hang", "don_vi_khaithac", "chi_nhanh_goc",
    "chi_nhanh_phat", "ma_buucuc_goc", "ma_buucuc_phat", "trong_luong", "loai_dv",
    "hanh_trinh", "tg_nhap_buucuc", "tg_laixe_nhan",
]

RAW_SCHEMA_OVERRIDES: Mapping[str, pl.DataType] = {
    "ma_phieugui": pl.String(),
    "ma_tai": pl.String(),
    "don_hoan": pl.Categorical(),
    "loai_hang": pl.Categorical(),
    "don_vi_khaithac": pl.Categorical(),
    "chi_nhanh_goc": pl.Categorical(),
    "chi_nhanh_phat": pl.Categorical(),
    "ma_buucuc_goc": pl.Categorical(),
    "ma_buucuc_phat": pl.Categorical(),
    "loai_dv": pl.Categorical(),
    "hanh_trinh": pl.Categorical(),
}


RD_SCHEMA_OVERRIDES: Mapping[str, pl.DataType] = {
    "don_vi_khai_thac": pl.Categorical(),
    "buu_cuc_phat": pl.Categorical(),
    "thoigian_nhapdau": pl.Time(),
    "thoigian_nhapcuoi": pl.Time(),
    "thoigian_xuat": pl.Time(),
    "ngay_xuat": pl.Int8(),
}

KN_SCHEMA_OVERRIDES: Mapping[str, pl.DataType] = {
    "don_vi_khai_thac": pl.Categorical(),
    "chi_nhanh_phat": pl.Categorical(),
    "thoigian_nhapdau": pl.Time(),
    "thoigian_nhapcuoi": pl.Time(),
    "thoigian_xuat": pl.Time(),
    "ngay_xuat": pl.Int8(),
}

LOOKUP_SCHEMA_OVERRIDES: Mapping[str, pl.DataType] = {
    "ma_buucuc": pl.Categorical(),
    "ma_tinh": pl.Categorical()
}

# ----- HELPER FUNCTIONS -----


@st.cache_data
def import_rule(file, rule_type: str) -> pl.DataFrame:
    schema_map = {
        "KN": KN_SCHEMA_OVERRIDES,
        "RD": RD_SCHEMA_OVERRIDES,
    }

    if rule_type not in schema_map:
        logger.error("Không tìm được loại rule")
        raise ValueError(f"Invalid rule type: {rule_type}")

    try:
        df = pl.read_excel(file, schema_overrides=schema_map[rule_type])
        # Pending validation logic
        return df
    except Exception as e:
        logger.error(f"Lỗi đọc rule: {e}")
        raise


@st.cache_data
def import_lookup(file) -> pl.DataFrame:
    try:
        df = pl.read_excel(file, schema_overrides=LOOKUP_SCHEMA_OVERRIDES)
        # Pending validation logic
        return df
    except Exception as e:
        logger.error(f"Lỗi đọc lookup: {e}")
        raise


def apply_rule(
    input_lf: pl.LazyFrame,
    rule_df: pl.LazyFrame,
    type: str
) -> pl.LazyFrame:

    # Determine join keys
    if type == "KN":
        left_on = ["don_vi_khaithac", "chi_nhanh_phat"]
        right_on = ["don_vi_khai_thac", "chi_nhanh_phat"]
    else:
        left_on = ["don_vi_khaithac", "ma_buucuc_phat"]
        right_on = ["don_vi_khai_thac", "buu_cuc_phat"]

    # Join
    lf = input_lf.join(rule_df, how="left", left_on=left_on, right_on=right_on)

    # Key matched flag
    lf = lf.with_columns(pl.col("thoigian_nhapdau").is_not_null().alias("_key_matched"))

    # Extract current time
    lf = lf.with_columns(pl.col("tg_nhap_buucuc").dt.time().alias("_current_time"))

    # Time range match
    lf = lf.with_columns(
        pl.when(pl.col("_key_matched"))
        .then(
            (pl.col("_current_time") >= pl.col("thoigian_nhapdau"))
            & (pl.col("_current_time") <= pl.col("thoigian_nhapcuoi"))
        )
        .otherwise(False)
        .alias("_time_matched")
    )

    # Compute deadline
    lf = lf.with_columns(
        [
            (pl.col("thoigian_xuat") - pl.time(0, 0, 0)).alias("_period"),
            (pl.when(pl.col("_time_matched"))
                .then(
                    pl.col("tg_nhap_buucuc").dt.truncate("1d") # Ngày nhập tại 00h00
                    + pl.col("_period") # Giờ deadline
                    + pl.duration(days=pl.col("ngay_xuat")) # Ngày offset
                )
                .otherwise(None)
                .alias("Deadline"))
        ]
    )

    # Gán nhãn:
    # - Nếu không tìm thấy cặp key thì rule đang thiếu → Check lại
    # - Nếu thấy cặp key, nhưng không có khung thời gian nào hợp lệ → Thiếu config
    # - Nếu thời gian lái xe nhận (thời gian xuất kho) <= deadline → Đúng
    # - Còn lại là sai hẹn
    lf = lf.filter(
        (~pl.col("_key_matched")) | (pl.col("_key_matched") & pl.col("_time_matched"))
    ).with_columns(
        pl.when(pl.col("_key_matched").not_())
        .then(pl.lit("Check lại"))
        .when(pl.col("_time_matched").not_())
        .then(pl.lit("Thiếu config"))
        .when(pl.col("tg_laixe_nhan") <= pl.col("Deadline"))
        .then(pl.lit("Đúng"))
        .otherwise(pl.lit("Sai hẹn"))
        .alias("Kết quả")
    )

    # Bỏ các dòng không cần thiết
    lf = lf.drop(["_key_matched", "_time_matched", "_period", "_current_time"])

    return lf


def process_HUB(
    input_lf: pl.LazyFrame,
    lookup: pl.DataFrame,
    rule_kn: pl.DataFrame,
    rule_rd: pl.DataFrame,
) -> Dict[str, pl.LazyFrame]:
    """Pipeline cho XS kho vùng tỉnh (HUB)"""

    processed = {}

    # Xác định HUB chi nhánh dựa theo 3 chữ cuối của đơn vị khai thác: "HUBDNG" → "DNG"
    lf = input_lf.with_columns(
        pl.col("don_vi_khaithac").cast(pl.String).str.slice(3, 3).alias("chi_nhanh_HUB")
    )

    ## Thay thế chi nhánh theo CONFIG
    if LOOKUP_OVERRIDES:
        expr = pl.col("chi_nhanh_HUB")
        for match_value, new_value in LOOKUP_OVERRIDES.items():
            expr = (
                pl.when(pl.col("don_vi_khaithac") == match_value)
                .then(new_value)
                .otherwise(expr)
            )
            lf = lf.with_columns(expr.cast(pl.Categorical).alias("chi_nhanh_HUB"))

    ## Join bảng lookup, ghi đè cột chi_nhanh_phat ở bảng cũ
    lf = (
        lf.join(
            lookup.lazy(), how="left", left_on="ma_buucuc_phat", right_on="ma_buucuc"
        )
        .with_columns(pl.col("ma_tinh").alias("chi_nhanh_phat"))
        .drop(["ma_tinh"])
    )

    lf = lf.with_columns(
        pl.when(pl.col("chi_nhanh_HUB") == pl.col("chi_nhanh_phat"))
        .then(pl.lit("RD"))
        .otherwise(pl.lit("KN"))
        .alias("phan_loai")
        .cast(pl.Categorical)
    )

    processed["RD"] = apply_rule(lf.filter(pl.col("phan_loai") == "RD"), rule_rd.lazy(), "RD")
    processed["KN"] = apply_rule(lf.filter(pl.col("phan_loai") == "KN"), rule_kn.lazy(), "KN")

    return processed


def process_TTKT(
    lf: pl.LazyFrame,
    rule: pl.DataFrame,
) -> pl.LazyFrame:

    # Áp rule dạng Rải đích
    lf = apply_rule(lf, rule.lazy(), "RD")
    return lf


def processing_pipeline(raw_files, raw_format, report_type):
    st.session_state.processing = True
    st.info("Processing in progress...")
    try:
        ## Init temp dir
        run_timestamp = time.strftime("%H%M_%d%m%Y")
        tmp_dir = tempfile.mkdtemp()

        ## Import loopup and rules
        rule_rd = import_rule(st.session_state.config_data["rule_rd"], "RD")
        rule_kn = import_rule(st.session_state.config_data["rule_kn"], "KN")
        lookup = import_lookup(st.session_state.config_data["lookup"])

        ## Scan data to lazyframe

        if raw_format == "xlsx":
            csv_paths = ingest_excel(raw_files, tmp_dir=tmp_dir)
        elif raw_format == "csv":
            csv_paths = raw_files

        lf = pl.scan_csv(
            csv_paths, columns=RAW_COLUMNS, schema_overrides=RAW_SCHEMA_OVERRIDES
        )

        ## Prefilter, cast datatype

        lf = lf.select(RAW_COLUMNS)

        typed = lf.with_columns(
            [
                pl.col("trong_luong").cast(pl.Float32),
                pl.col("tg_nhap_buucuc").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                pl.col("tg_laixe_nhan").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            ]
        )

        ## Main processing according to report type

        if st.session_state.config_data["output_format"] == "csv"
            out_path = st.session_state.config_data["output_folder"]
        else:
            out_path = tmp_dir

        if report_type == "Xuất sạch Kho vùng tỉnh (HUB)":
            processed = process_HUB(typed, lookup, rule_kn, rule_rd)
            processed["RD"].sink_csv(os.path.join(out_path,f"XuatsachHUB-Raidich-{run_timestamp}.csv"))
            processed["KN"].sink_csv(os.path.join(out_path,f"XuatsachHUB-Ketnoi-{run_timestamp}.csv"))
        else:
            processed = process_TTKT(typed, rule_rd)
            processed.sink_csv(os.path.join(out_path,f"XuatsachTTKT-{run_timestamp}.csv"))

        ## Final convert if output path is excel
        if st.session_state.config_data["output_format"] == "excel":
            for file in os.listdir(tmp_dir):
                df = pl.read_csv(os.path.join(tmp_dir, file))
                excel_filename = file.replace(".csv", ".xlsx")
                excel_filepath = os.path.join(st.session_state.config_data["output_folder"], excel_filename)
                df.write_excel(excel_filepath)

    except Exception as e:
        logger.error(f"Error processing files: {e}")
        st.error(f"❌ Error during processing: {str(e)}")

        import traceback

        st.code(traceback.format_exc())
    finally:
        st.session_state.processing = False

        # Cleanup
        os.remove(tmp_dir)
