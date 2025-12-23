import os
from dataclasses import dataclass
import time
import polars as pl
from io import BytesIO
from typing import Dict

from etl.ingest import import_files

@dataclass(frozen=True)
class PipelineResult:
    rows_in: int
    rows_out: int
    output_files: list[str]

# --- Config ---

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

COLS_XUAT_SACH_HUB = [
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
#     "CNHUB",
#     "Miền",
#     "phan_loai",
#     "Result_p",
#     "deadline",
#     "timedelta"
]

COLS_XUAT_SACH_TTKT = [
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
#     "CNHUB",
#     "Miền",
#     "phan_loai",
#     "Result_p",
#     "deadline",
#     "timedelta"
]

# --- Helper functions ---

def import_rule(file_path: str, rule_type: str) -> pl.DataFrame:
    df = pl.read_excel(file_path, schema_overrides=RULE_SCHEMA_OVERRIDES)
    # Pending validation logic
    return df
    

def import_lookup(file_path: str) -> pl.DataFrame:
    df = pl.read_excel(file_path)
    # Pending validation logic
    return df
    

def apply_rule(lf: pl.LazyFrame, rule: pl.LazyFrame, type: str) -> pl.LazyFrame:
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
        .alias("deadline")
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
        .when(pl.col("tg_laixe_nhan") <= pl.col("deadline"))
        .then(pl.lit("Đúng"))
        .otherwise(pl.lit("Sai hẹn"))
        .alias("Result_p")
    )

    # Bỏ các dòng không cần thiết
    lf = lf.drop(
        ["_key_matched", "_time_matched", "_deadline_period", "_enter_time"]
    )

    return lf


def pipeline_xs_hub(
    input_files: list[BytesIO],
    config: Dict
) -> Dict:
    """
    Pipeline Xuất sạch HUB
    "common": {
        "thamchieu_noitinh": null
    },
    "xuat_sach_hub": {
        "rule_rd_folder": null,
        "rule_rd_file": null,
        "rule_kn_folder": null,
        "rule_kn_file": null,
        "output_rd_folder": "output",
        "output_kn_folder": "output"
    },
    "pipeline_options": {
        "pipeline_select": null,
        "fast_mode": false
    }
    """
    # Load options
    lookup_path = config["common"]["thamchieu_noitinh"]
    opts = config["xuat_sach_hub"]
    pipeline_cfg = config["pipeline_options"]

    # Load rules & lookup
    rule_rd_path = os.path.join(opts["rule_rd_folder"], opts["rule_rd_file"])
    lf_rule_rd = import_rule(rule_rd_path, "RD").lazy()

    rule_kn_path = os.path.join(opts["rule_kn_folder"], opts["rule_kn_file"])
    lf_rule_kn = import_rule(rule_kn_path, "KN").lazy()

    lf_lookup = import_lookup(lookup_path).lazy()

    # Ingest raw
    import_result = import_files(input_files, pipeline_cfg["fast_mode"])
    df = import_result.df
    rows_in = df.height

    # Keep needed columns
    df = df.select(COLS_XUAT_SACH_TTKT)

    # Correct datetime datatype
    df = df.with_columns(
        [
            pl.col("tg_nhap_buucuc").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("tg_laixe_nhan").str.to_datetime("%Y-%m-%d %H:%M:%S"),
        ]
    )
    lf = df.lazy()

    # Transformation

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
            lf_lookup, how="left", left_on="ma_buucuc_phat", right_on="ma_buucuc"
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

    # --- Export ---

    fn_map = {"RD": "RaiDich", "KN": "KetNoi"}
    output_path = {"RD": opts["output_rd_folder"], "KN": opts["output_kn_folder"]}
    output_files = []
    rows_out = 0

    if import_result.date == "":
        export_suffix = time.strftime("%Y%m%d_%H%M%S")
    else:
        export_suffix = import_result.date
        
    for type in outputs.keys():
        rows_out += outputs[type].select(pl.len()).collect().item()
        file_name = f"XuatsachHUB{fn_map[type]}_{export_suffix}.csv"
        outputs[type].sink_csv(
            os.path.join(output_path[type], file_name)
        )
        output_files.append(file_name)

    return {
        'rows_in': rows_in,
        'rows_out': rows_out,
        'output_files': output_files
    }

def pipeline_xs_ttkt(
    input_files: list[BytesIO],
    config: Dict
) -> Dict:
    """
    Pipeline Xuất sạch TTKT
    
    "common": {
        "thamchieu_noitinh": null
    },
    "xuat_sach_ttkt": {
        "rule_folder": null,
        "rule_file": null,
        "output_folder": "output"
    },
    "pipeline_options": {
        "pipeline_select": null,
        "fast_mode": false
    }

    """
    # Load options
    lookup_path = config["common"]["thamchieu_noitinh"]
    opts = config["xuat_sach_ttkt"]
    pipeline_cfg = config["pipeline_options"]

    # Load rules & lookup
    rule_path = os.path.join(opts["rule_folder"], opts["rule_file"])
    rule = import_rule(rule_path, "RD") # Rule tương tự rule Rải đích
    rule_lf = rule.lazy()

    lookup = import_lookup(lookup_path)
    lookup_lf = lookup.lazy()

    # Ingest raw
    import_result = import_files(input_files, pipeline_cfg["fast_mode"])
    df = import_result.df
    rows_in = df.height

    # Keep needed columns
    df = df.select(COLS_XUAT_SACH_TTKT)

    # Correct datetime datatype
    df = df.with_columns(
        [
            pl.col("tg_nhap_buucuc").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            pl.col("tg_laixe_nhan").str.to_datetime("%Y-%m-%d %H:%M:%S"),
        ]
    )
    lf = df.lazy()

    # Tham chiếu miền phát từ bưu cục phát
    lf = lf.join(lookup_lf, how="left", left_on="ma_buucuc_phat", right_on="ma_buucuc").drop("ma_tinh")

    # Tìm rule và deadline phù hợp với mỗi đơn (Tương tự rule rải đích)
    lf = apply_rule(lf, rule=rule_lf, type="RD")

    # Thêm timedelta
    lf = lf.with_columns(
        pl.when(pl.col("Result_p") == "Sai hẹn").then((pl.col("tg_laixe_nhan") - pl.col("deadline"))).alias("_time_delta")
    )

    lf = lf.with_columns([
        (pl.col("_time_delta").abs().dt.total_seconds() // (24 * 60 * 60)).alias("days"),  # Calculate days
        (pl.col("_time_delta").abs().dt.total_seconds() // 3600 % 24).alias("hours"),   # Calculate hours
        (pl.col("_time_delta").abs().dt.total_seconds() // 60 % 60).alias("minutes"),    # Calculate minutes
        (pl.col("_time_delta").abs().dt.total_seconds() % 60).alias("seconds")
    ])

    lf = lf.with_columns(
        pl.concat_str([
            pl.col("days").cast(pl.Utf8),
            pl.lit("."),
            pl.col("hours").cast(pl.Utf8).str.zfill(2),
            pl.lit(":"),
            pl.col("minutes").cast(pl.Utf8).str.zfill(2),
            pl.lit(":"),
            pl.col("seconds").cast(pl.Utf8).str.zfill(2),
        ]).alias("timedelta")
    )
    # Tạo các cột trống làm placeholder
    lf = lf.with_columns(
        [
            pl.lit(None).alias("CNHUB"),
            pl.lit(None).alias("phan_loai"),
        ]
    )

    # --- Export ---

    if import_result.date == "":
        export_suffix = time.strftime("%Y%m%d_%H%M%S")
    else:
        export_suffix = import_result.date

    file_name = f"XuatsachTTKT_{export_suffix}.csv"

    # Count rows
    rows_out = lf.select(pl.len()).collect().item()

    # Write csv
    lf.sink_csv(
        os.path.join(opts["output_folder"], file_name),
        datetime_format="%Y-%m-%d %H:%M:%S",
        date_format= "%Y-%m-%d",
        time_format="%H:%M:%S"
    )

    return {
        'rows_in': rows_in,
        'rows_out': rows_out,
        'output_files': file_name
    }