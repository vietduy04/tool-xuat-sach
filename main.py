from operator import index
import os
import time
import pandas as pd
import numpy as np
from datetime import timedelta
from utils.logger import log
import tempfile

# ---------- Configurations ---------- (Tên file rule RD, KN)
rd_path = "RD.xlsx"
kn_path = "KN.xlsx"
CHUNK_SIZE = 100000

# ---------- Paths ----------
current_dir = os.getcwd()
raw_path = os.path.join(current_dir, "data", "raw")
output_path = os.path.join(current_dir, "output")
lookup_path = os.path.join(current_dir, "data", "lookup", "lookup.xlsx")
rules_path = os.path.join(current_dir, "data", "rules")
test_path = os.path.join(current_dir, "test_data")

RAW_COLUMNS = [
    'ma_phieugui','ma_tai','don_hoan','loai_hang','trong_luong','ma_buucuc_goc',
    'ma_buucuc_phat','chi_nhanh_goc','chi_nhanh_phat','don_vi_khaithac','dvc',
    'ma_dv_viettel','loai_dv','type','id_ht_tuyenxe','trip_picked_code','hanh_trinh',
    'time_expect','trip_code_expect','trip_name_expect','trip_id_expect','thoi_gian_expect',
    'tg_nhap_buucuc','tg_laixe_nhan','arrival_time_reality','leave_time_reality',
    'kpi_xuat_sach','chinhanh_dvkt','mien_dvkt','phanloai_hang','bien_kiemsoat',
    'chuyen_tuyen','buucuc_xahoi','buucuc_1ca','deadline_htdt','dg_htdt','deadline_cldv',
    'dg_cldv'
]

CLEANED_COLUMNS = [
    'ma_phieugui', 'ma_tai', 'don_hoan', 'loai_hang', 'don_vi_khaithac', 'chi_nhanh_goc', 
    'chi_nhanh_phat', 'ma_buucuc_goc', 'ma_buucuc_phat', 'trong_luong', 'loai_dv', 
    'hanh_trinh', 'tg_nhap_buucuc', 'tg_laixe_nhan']

# ---------- Helpers ----------
def preprocess_rules(rule_df):
    """Convert rule time columns to seconds and ngay_xuat to timedelta."""
    # ensure time columns are parsed as datetime.time
    for tcol in ['thoigian_nhapdau','thoigian_nhapcuoi','thoigian_xuat']:
        rule_df[tcol] = pd.to_datetime(rule_df[tcol], format='%H:%M:%S').dt.time

    rule_df['thoigian_nhapdau_s'] = rule_df['thoigian_nhapdau'].apply(lambda t: t.hour*3600 + t.minute*60 + t.second)
    rule_df['thoigian_nhapcuoi_s'] = rule_df['thoigian_nhapcuoi'].apply(lambda t: t.hour*3600 + t.minute*60 + t.second)
    rule_df['thoigian_xuat_s'] = rule_df['thoigian_xuat'].apply(lambda t: t.hour*3600 + t.minute*60 + t.second)

    # convert ngay_xuat to timedelta (days)
    rule_df['ngay_xuat_td'] = rule_df['ngay_xuat'].astype(int).apply(lambda x: timedelta(days=int(x)))

    return rule_df

def compute_result_for_subset(df_subset, rules, left_on, right_on):
    """
    Vectorized join between df_subset and rules.
    Returns a Series indexed by original row index with values: 'Đúng','Sai hẹn','Thiếu config','Check lại'.
    """
    if df_subset.empty:
        return pd.Series(dtype='object')

    # keep original index
    df_subset = df_subset.copy()
    df_subset['__idx'] = df_subset.index

    # compute seconds for tg_nhap_buucuc
    df_subset['_tg_dt'] = pd.to_datetime(df_subset['tg_nhap_buucuc'], format='%Y-%m-%d %H:%M:%S')
    df_subset['tg_nhap_s'] = df_subset['_tg_dt'].dt.hour*3600 + df_subset['_tg_dt'].dt.minute*60 + df_subset['_tg_dt'].dt.second

    # merge many-to-many on keys
    merged = df_subset.merge(
        rules,
        how='left',
        left_on=left_on,
        right_on=right_on,
        suffixes=('','_rule'),
        copy=False
    )

    # mark which rows had any matched rule (size>0 after merge will indicate that)
    # For unmatched rows merge will have NaNs in rule key columns
    merged['matched_rule'] = ~merged['thoigian_nhapdau'].isna()

    # time match
    merged['time_match'] = merged['matched_rule'] & (
        (merged['thoigian_nhapdau_s'] <= merged['tg_nhap_s']) &
        (merged['tg_nhap_s'] <= merged['thoigian_nhapcuoi_s'])
    )

    # compute expected datetime and flag only where time_match True
    # build expected datetime = date(tg_nhap_buucuc) + thoigian_xuat_s + ngay_xuat_td
    if 'thoigian_xuat_s' in merged.columns:
        date_part = pd.to_datetime(merged['tg_nhap_buucuc']).dt.normalize()
        merged['expected_dt'] = date_part + pd.to_timedelta(merged['thoigian_xuat_s'], unit='s') + merged['ngay_xuat_td'].fillna(timedelta(0))
        # For rows without time_match, we'll set flag False
        merged['flag'] = merged['time_match'] & (pd.to_datetime(merged['tg_laixe_nhan']) <= merged['expected_dt'])
    else:
        merged['flag'] = False

    # Aggregations per original row index
    agg = merged.groupby('__idx').agg(
        matched_rules_count = ('matched_rule','sum'),
        any_time_match = ('time_match','max'),
        any_flag_true = ('flag','max'), 
        expected_dt = ('expected_dt','last')
    ).reindex(df_subset.index, fill_value=0)

    # determine final result per index
    # matched_rules_count==0 -> 'Check lại'
    # matched_rules_count>0 and any_time_match==0 -> 'Thiếu config'
    # any_time_match==1 and any_flag_true==1 -> 'Đúng'
    # any_time_match==1 and any_flag_true==0 -> 'Sai hẹn'
    res = pd.Series(index=agg.index, dtype='object')
    res[agg['matched_rules_count']==0] = 'Check lại'
    mask_have_rules = agg['matched_rules_count']>0
    res[mask_have_rules & (~agg['any_time_match'].astype(bool))] = 'Thiếu config'
    res[mask_have_rules & (agg['any_time_match'].astype(bool)) & (agg['any_flag_true'].astype(bool))] = 'Đúng'
    res[mask_have_rules & (agg['any_time_match'].astype(bool)) & (~agg['any_flag_true'].astype(bool))] = 'Sai hẹn'
    return res, agg['expected_dt']

# ---------- Main processing per file ----------

def convert_excel_to_csv(excel_path):
    df = pd.read_excel(excel_path, engine='calamine', skiprows=2, header=None)
    df.columns = RAW_COLUMNS
    df = df[CLEANED_COLUMNS]

    temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    df.to_csv(temp_csv, index=False, header=False)
    return temp_csv

def process_chunk(df, lookup_df, rule_RD, rule_KN, rd_output_file, kn_output_file, write_header=False):
    # Assign columns
    df.columns = CLEANED_COLUMNS
    # vectorized CNHUB
    df['CNHUB'] = df['don_vi_khaithac'].str[3:6]
    df.loc[df['don_vi_khaithac']=='HUBTAN','CNHUB'] = 'BDG'
    df.loc[df['don_vi_khaithac']=='HUBBHD','CNHUB'] = 'BDH'
    
    # merge lookup once per chunk, replacing chi_nhanh_phat
    df = df.merge(lookup_df, how='left', left_on='ma_buucuc_phat', right_on='ma_buucuc')
    df['chi_nhanh_phat'] = df['ma_tinh']
    df.drop(columns=['ma_tinh','ma_buucuc'], inplace=True)

    # vectorized loai_hang
    df['phan_loai'] = np.where(df['CNHUB'] == df['chi_nhanh_phat'], 'RD', 'KN')
    # parse datetime columns
    df['tg_nhap_buucuc'] = pd.to_datetime(df['tg_nhap_buucuc'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    df['tg_laixe_nhan'] = pd.to_datetime(df['tg_laixe_nhan'], errors='coerce', format='%Y-%m-%d %H:%M:%S')

    # prepare result column default 'Check lại' & NaT (will be overwritten)
    df['Result_p'] = 'Check lại'
    df['deadline'] = pd.NaT

    # store original index for mapping
    df.index.name = 'orig_idx'

    # RD subset
    df_RD = df[df['phan_loai']=='RD']
    if not df_RD.empty:
        res_RD, deadline_RD = compute_result_for_subset(df_RD, rule_RD, left_on=['don_vi_khaithac','ma_buucuc_phat'], right_on=['don_vi_khai_thac','buu_cuc_phat'])
        df.loc[res_RD.index, 'Result_p'] = res_RD
        df.loc[deadline_RD.index, 'deadline'] = deadline_RD

    # KN subset
    df_KN = df[df['phan_loai']=='KN']
    if not df_KN.empty:
        res_KN, deadline_KN = compute_result_for_subset(df_KN, rule_KN, left_on=['don_vi_khaithac','chi_nhanh_phat'], right_on=['don_vi_khai_thac','chi_nhanh_phat'])
        df.loc[res_KN.index, 'Result_p'] = res_KN
        df.loc[deadline_KN.index, 'deadline'] = deadline_KN

    # write to each subset (RD/KN) to CSV (append mode)
    df_RD = df[df['phan_loai']=='RD']
    df_KN = df[df['phan_loai']=='KN']

    df_RD.to_csv(rd_output_file, mode='a', header=write_header, index=False, encoding='utf-8')
    df_KN.to_csv(kn_output_file, mode='a', header=write_header, index=False, encoding='utf-8')

    # cols_to_write = list(df.columns)
    # df.to_csv(output_csv, mode='a', header=write_header, index=False, encoding='utf-8')
    return df.shape[0]

# ---------- Entrypoint ----------
if __name__ == "__main__":
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # output_file = os.path.join(output_path, "output.csv")
    # # remove if exists to start fresh
    # if os.path.exists(output_file):
    #     os.remove(output_file)

    log("Starting processing files...")
    # load lookup and rules once
    lookup_df = pd.read_excel(lookup_path, engine='calamine')

    rule_RD = pd.read_excel(os.path.join(rules_path, rd_path), engine='calamine')
    rule_KN = pd.read_excel(os.path.join(rules_path, kn_path), engine='calamine')

    rule_RD = preprocess_rules(rule_RD)
    rule_KN = preprocess_rules(rule_KN)

    total_rows = 0
    first_write = True

    # create blank output files
    run_timestamp = time.strftime("%H%M_%d%m%Y")
    rd_output_file = os.path.join(output_path, f"RD_{run_timestamp}.csv")
    kn_output_file = os.path.join(output_path, f"KN_{run_timestamp}.csv")

    for fn in os.listdir(raw_path):
        if not fn.endswith('.xlsx'):
            continue
        log(f"Processing file: {fn}")
        # Convert to csv
        csv_path = convert_excel_to_csv(os.path.join(raw_path, fn))
        for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE, header=None):
            processed_chunk = process_chunk(chunk, lookup_df, rule_RD, rule_KN, rd_output_file, kn_output_file, write_header=first_write)
            if first_write:
                first_write = False
            total_rows += processed_chunk
            log(f"Processed {total_rows} total rows so far.")
    log(f"Total processed rows: {total_rows}. Processing completed.")