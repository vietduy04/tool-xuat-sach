"""Pipeline Xuất sạch kho vùng tỉnh (HUB) & TTKT"""

import os

import time
from pathlib import Path

import polars as pl
import streamlit as st
from utils.logger import get_logger

from etl.ingest import ingest_files

logger = get_logger()

# ----- PIPELINE SPECIFIC CONFIG -----

HUB_OVERRIDES = {
    # don_vi_khai_thac: chi_nhanh_HUB
    "HUBTAN": "BDG",
    "HUBBHD": "BDH",
}

RULE_SCHEMA_OVERRIDES = {
    "thoigian_nhapdau": pl.Time(),
    "thoigian_nhapcuoi": pl.Time(),
    "thoigian_xuat": pl.Time(),
    "ngay_xuat": pl.Int8(),
}

# ----- HELPER FUNCTIONS -----


@st.cache_data
def import_rule(file) -> pl.LazyFrame:
    try:
        df = pl.read_excel(file, schema_overrides=RULE_SCHEMA_OVERRIDES)
        # Pending validation logic
        return df.lazy()
    except Exception as e:
        logger.error(f"Lỗi đọc rule: {e}")
        raise


@st.cache_data
def import_lookup(file) -> pl.LazyFrame:
    try:
        df = pl.read_excel(file)
        # Pending validation logic
        return df.lazy()
    except Exception as e:
        logger.error(f"Lỗi đọc lookup: {e}")
        raise


def apply_rule(lf: pl.LazyFrame, rule: pl.LazyFrame, type: str) -> pl.LazyFrame:
    try:
        # Join keys
        join_keys_mapping = {
            "RD": {
                "left_on": ["don_vi_khaithac", "ma_buucuc_phat"],
                "right_on": ["don_vi_khai_thac", "buu_cuc_phat"],
            },
            "KN": {
                "left_on": ["don_vi_khaithac", "chi_nhanh_phat"],
                "right_on": ["don_vi_khai_thac", "chi_nhanh_phat"],
            },
        }

        keys = join_keys_mapping[type]

        # Join
        lf = lf.join(
            rule, how="left", left_on=keys["left_on"], right_on=keys["right_on"]
        )

        # Flag khớp key
        lf = lf.with_columns(
            pl.col("thoigian_nhapdau").is_not_null().alias("_key_matched")
        )

        # Lấy thời gian nhập để so sánh
        lf = lf.with_columns(pl.col("tg_nhap_buucuc").dt.time().alias("_enter_time"))

        # Flag khớp khoảng giờ nhập
        lf = lf.with_columns(
            pl.when(pl.col("_key_matched"))
            .then(
                (pl.col("_enter_time") >= pl.col("thoigian_nhapdau"))
                & (pl.col("_enter_time") <= pl.col("thoigian_nhapcuoi"))
            )
            .otherwise(False)
            .alias("_time_matched")
        )

        # Tính deadline (ngày nhập + giờ deadline + ngày offset)
        lf = lf.with_columns(
            (pl.col("thoigian_xuat") - pl.time(0, 0, 0)).alias("_deadline_period")
        )

        lf = lf.with_columns(
            pl.when(pl.col("_time_matched"))
            .then(
                pl.col("tg_nhap_buucuc").dt.truncate("1d")  # Ngày nhập tại 00h00
                + pl.col("_deadline_period")  # Giờ deadline
                + pl.duration(days=pl.col("ngay_xuat"))  # Ngày offset
            )
            .otherwise(None)
            .alias("Deadline")
        )

        # Gán nhãn:
        # - Nếu không tìm thấy cặp key thì rule đang thiếu → Check lại
        # - Nếu thấy cặp key, nhưng không có khung thời gian nào hợp lệ → Thiếu config
        # - Nếu thời gian lái xe nhận (thời gian xuất kho) <= deadline → Đúng
        # - Còn lại là sai hẹn
        lf = lf.filter(
            (~pl.col("_key_matched"))
            | (pl.col("_key_matched") & pl.col("_time_matched"))
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
        lf = lf.drop(
            ["_key_matched", "_time_matched", "_deadline_period", "_enter_time"]
        )

        return lf
    except Exception as e:
        logger.error(f"Lỗi không áp được rule: {e}")
        raise


def run_pipeline(type: str, raw_files: list, config: dict):
    output_path = Path(config["output_path"])
    output_path.mkdir(exist_ok=True)
    pipelines = {
        "Xuất sạch TTKT": pipeline_TTKT(raw_files, config),
        "Xuất sạch Kho vùng tỉnh (HUB)": pipeline_HUB(raw_files, config),
    }

    pipelines[type]


def pipeline_HUB(raw_files: list, config: dict):
    st.session_state.processing = True
    timestamp = time.strftime("%H%M%S_%d%m%Y")
    try:
        with st.status("Bắt đầu luồng xử lý...", expanded=True) as s:
            start = time.perf_counter()
            s.update(label="Load config...")
            raw_format = config["raw_format"]
            output_path = config["output_path"]

            s.update(label="Load rules & tham chiếu...")
            rule_rd = import_rule(config["rule_rd"])
            lf_rule_rd = rule_rd.lazy()

            rule_kn = import_rule(config["rule_kn"])
            lf_rule_kn = rule_kn.lazy()

            lookup = import_lookup(config["lookup"])
            lf_lookup = lookup.lazy()

            s.update(label="Load file raw...")
            lf = ingest_files(raw_files, raw_format)
            lf = lf.with_columns(
                [
                    pl.col("tg_nhap_buucuc").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                    pl.col("tg_laixe_nhan").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                ]
            )

            s.update(label="Áp rule...")

            lf = lf.with_columns(
                pl.col("don_vi_khaithac").str.slice(3, 3).alias("chi_nhanh_HUB")
            )

            # Xác định chi nhánh hiện tại theo đơn vị khai thác
            lf = lf.with_columns(
                pl.col("don_vi_khaithac").str.slice(3, 3).alias("chi_nhanh_HUB")
            )

            # Thay thế theo HUB_OVERRIDES
            if HUB_OVERRIDES:
                expr = pl.col("chi_nhanh_HUB")
                for match_value, new_value in HUB_OVERRIDES.items():
                    expr = (
                        pl.when(pl.col("don_vi_khaithac") == match_value)
                        .then(pl.lit(new_value))
                        .otherwise(expr)
                    )
                lf = lf.with_columns(expr.alias("chi_nhanh_HUB"))

            # Xác định chi nhánh phát cũ theo lookup
            lf = (
                lf.join(
                    lf_lookup,
                    how="left",
                    left_on="ma_buucuc_phat",
                    right_on="ma_buucuc",
                )
                .drop("chi_nhanh_phat")
                .rename({"ma_tinh": "chi_nhanh_phat"})
            )

            # Phân loại đơn rải đích / kết nối
            lf = lf.with_columns(
                pl.when(pl.col("chi_nhanh_HUB") == pl.col("chi_nhanh_phat"))
                .then(pl.lit("RD"))
                .otherwise(pl.lit("KN"))
                .alias("phan_loai")
                .cast(pl.Categorical)
            )

            # Tìm rule và deadline phù hợp với mỗi đơn
            rules = {"RD": lf_rule_rd, "KN": lf_rule_kn}
            outputs = {"RD": pl.LazyFrame(), "KN": pl.LazyFrame()}

            for type in outputs.keys():
                filtered = lf.filter(pl.col("phan_loai") == type)
                outputs[type] = apply_rule(lf=filtered, rule=rules[type], type=type)

            s.update(label="Xuất kết quả...")

            fn_map = {"RD": "RaiDich", "KN": "KetNoi"}

            for type in outputs.keys():
                outputs[type].sink_csv(
                    os.path.join(
                        output_path, f"XuatsachHUB-{fn_map[type]}-{timestamp}.csv"
                    ),
                    datetime_format="%Y-%m-%d %H:%M:%S",
                    date_format="%Y-%m-%d",
                    time_format="%H:%M:%S",
                )

            s.update(
                label=f"Hoàn thành! --- `{time.perf_counter() - start}s`",
                state="complete",
            )
            # s.write(f"File kết quả: {full_output_path}")

    except Exception as e:
        st.error(f"Pipeline lỗi: {e}")

        import traceback

        st.code(traceback.format_exc())
    finally:
        st.session_state.processing = False


def pipeline_TTKT(raw_files: list, config: dict):
    st.session_state.processing = True
    timestamp = time.strftime("%H%M%S_%d%m%Y")
    try:
        with st.status("Bắt đầu luồng xử lý...", expanded=True) as s:
            start = time.perf_counter()
            s.update(label="Load config...")
            raw_format = config["raw_format"]
            output_path = config["output_path"]

            s.update(label="Load rules...")
            rule = import_rule(config["rule_ttkt"])
            lf_rule = rule.lazy()

            s.update(label="Load file raw...")
            lf = ingest_files(raw_files, raw_format)
            lf = lf.with_columns(
                [
                    pl.col("tg_nhap_buucuc").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                    pl.col("tg_laixe_nhan").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                ]
            )

            s.update(label="Áp rule...")
            lf = apply_rule(lf, rule=lf_rule, type="RD")

            s.update(label="Xuất kết quả...")
            full_output_path = os.path.join(
                output_path, f"XuatsachTTKT@{timestamp}.csv"
            )
            lf.sink_csv(
                full_output_path,
                datetime_format="%Y-%m-%d %H:%M:%S",
                date_format="%Y-%m-%d",
                time_format="%H:%M:%S",
            )

            s.write(f"File kết quả: {full_output_path}")
            s.update(
                label=f"Hoàn thành! --- `{time.perf_counter() - start}s`",
                state="complete",
            )

    except Exception as e:
        st.error(f"Pipeline lỗi: {e}")

        import traceback

        st.code(traceback.format_exc())
    finally:
        st.session_state.processing = False


