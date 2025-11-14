"""Processing module: Calculations, joins, and rule application."""
import pandas as pd
import numpy as np
from datetime import timedelta
from typing import Tuple, List
from utils.logger import get_logger

logger = get_logger()

REPORT_TYPE_TTKT = "Báo cáo XS TTKT"


def preprocess_rules(rule_df: pd.DataFrame) -> pd.DataFrame:
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
        for tcol in ['thoigian_nhapdau', 'thoigian_nhapcuoi', 'thoigian_xuat']:
            if tcol in rule_df.columns:
                rule_df[tcol] = pd.to_datetime(rule_df[tcol], format='%H:%M:%S').dt.time
        
        rule_df['thoigian_nhapdau_s'] = rule_df['thoigian_nhapdau'].apply(
            lambda t: t.hour * 3600 + t.minute * 60 + t.second
        )
        rule_df['thoigian_nhapcuoi_s'] = rule_df['thoigian_nhapcuoi'].apply(
            lambda t: t.hour * 3600 + t.minute * 60 + t.second
        )
        rule_df['thoigian_xuat_s'] = rule_df['thoigian_xuat'].apply(
            lambda t: t.hour * 3600 + t.minute * 60 + t.second
        )
        
        # Convert ngay_xuat to timedelta (days)
        if 'ngay_xuat' in rule_df.columns:
            rule_df['ngay_xuat_td'] = rule_df['ngay_xuat'].astype(int).apply(
                lambda x: timedelta(days=int(x))
            )
        
        return rule_df
    except Exception as e:
        logger.error(f"Error preprocessing rules: {e}")
        raise


def compute_result_for_subset(
    df_subset: pd.DataFrame,
    rules: pd.DataFrame,
    left_on: List[str],
    right_on: List[str]
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Vectorized join between df_subset and rules.
    
    Args:
        df_subset: Subset of data to process
        rules: Rules DataFrame
        left_on: Columns to join on from df_subset
        right_on: Columns to join on from rules
        
    Returns:
        Tuple of (result Series, deadline Series, timedelta Series)
    """
    if df_subset.empty:
        return (
            pd.Series(dtype='object'),
            pd.Series(dtype='datetime64[ns]'),
            pd.Series(dtype='string')
        )
    
    try:
        # Keep original index
        df_subset = df_subset.copy()
        df_subset['__idx'] = df_subset.index
        
        # Compute seconds for tg_nhap_buucuc
        df_subset['_tg_dt'] = pd.to_datetime(df_subset['tg_nhap_buucuc'], format='%Y-%m-%d %H:%M:%S')
        df_subset['tg_nhap_s'] = (
            df_subset['_tg_dt'].dt.hour * 3600 +
            df_subset['_tg_dt'].dt.minute * 60 +
            df_subset['_tg_dt'].dt.second
        )
        
        # Merge many-to-many on keys
        merged = df_subset.merge(
            rules,
            how='left',
            left_on=left_on,
            right_on=right_on,
            suffixes=('', '_rule'),
            copy=False
        )
        
        # Mark which rows had any matched rule
        merged['matched_rule'] = ~merged['thoigian_nhapdau'].isna()
        
        # Time match
        merged['time_match'] = merged['matched_rule'] & (
            (merged['thoigian_nhapdau_s'] <= merged['tg_nhap_s']) &
            (merged['tg_nhap_s'] <= merged['thoigian_nhapcuoi_s'])
        )
        
        # Compute expected datetime and flag only where time_match True
        if 'thoigian_xuat_s' in merged.columns:
            date_part = pd.to_datetime(merged['tg_nhap_buucuc']).dt.normalize()
            merged['expected_dt'] = (
                date_part +
                pd.to_timedelta(merged['thoigian_xuat_s'], unit='s') +
                merged['ngay_xuat_td'].fillna(timedelta(0))
            )
            # For rows without time_match, we'll set flag False
            merged['flag'] = merged['time_match'] & (
                pd.to_datetime(merged['tg_laixe_nhan']) <= merged['expected_dt']
            )
        else:
            merged['flag'] = False
        
        # Filter to only rows with time_match and ensure one match per original row
        matched_rows = merged[merged['time_match'] == True].copy()
        
        # If multiple matches exist for same __idx, keep only the first one
        if not matched_rows.empty:
            matched_rows = matched_rows.drop_duplicates(subset='__idx', keep='first')
        
        # Create mapping from __idx to matched rule data
        matched_dict = {}
        if not matched_rows.empty:
            matched_dict = matched_rows.set_index('__idx')[['expected_dt', 'flag']].to_dict('index')
        
        # Aggregations per original row index (for determining result status)
        agg = merged.groupby('__idx').agg(
            matched_rules_count=('matched_rule', 'sum'),
            any_time_match=('time_match', 'max')
        ).reindex(df_subset.index, fill_value=0)
        
        # Extract expected_dt and flag from matched rule (correct one, not aggregated)
        expected_dt_series = pd.Series(index=df_subset.index, dtype='datetime64[ns]')
        matched_flag_series = pd.Series(index=df_subset.index, dtype='bool')
        for idx in df_subset.index:
            if idx in matched_dict:
                expected_dt_series[idx] = matched_dict[idx]['expected_dt']
                matched_flag_series[idx] = matched_dict[idx]['flag']
            else:
                matched_flag_series[idx] = False
        
        # Determine final result per index
        res = pd.Series(index=agg.index, dtype='object')
        res[agg['matched_rules_count'] == 0] = 'Check lại'
        mask_have_rules = agg['matched_rules_count'] > 0
        res[mask_have_rules & (~agg['any_time_match'].astype(bool))] = 'Thiếu config'
        res[mask_have_rules & (agg['any_time_match'].astype(bool)) & matched_flag_series] = 'Đúng'
        res[mask_have_rules & (agg['any_time_match'].astype(bool)) & (~matched_flag_series)] = 'Sai hẹn'
        
        # Calculate timedelta only for "Sai hẹn" rows
        timedelta_series = pd.Series(index=df_subset.index, dtype='timedelta64[ns]')
        sai_hen_mask = res == 'Sai hẹn'
        timedelta_text = pd.Series(index=timedelta_series.index, dtype="string")
        timedelta_text[:] = ""

        if sai_hen_mask.any():
            tg_laixe_nhan = pd.to_datetime(df_subset.loc[sai_hen_mask, 'tg_laixe_nhan'])
            expected_dt_for_sai_hen = expected_dt_series[sai_hen_mask]
            # Calculate timedelta: tg_laixe_nhan - expected_dt (positive means late)
            timedelta_series[sai_hen_mask] = tg_laixe_nhan - expected_dt_for_sai_hen

            # Convert timedelta to string format "D.HH:MM:SS" for PowerQuery
            td = timedelta_series.dt.components
            
            # Format only valid rows
            timedelta_text[sai_hen_mask] = (
                td.loc[sai_hen_mask, "days"].astype("int64").astype(str)
                + "."
                + td.loc[sai_hen_mask, "hours"].astype("int64").astype(str).str.zfill(2)
                + ":"
                + td.loc[sai_hen_mask, "minutes"].astype("int64").astype(str).str.zfill(2)
                + ":"
                + td.loc[sai_hen_mask, "seconds"].astype("int64").astype(str).str.zfill(2)
            )

        return res, expected_dt_series, timedelta_text
    except Exception as e:
        logger.error(f"Error computing result for subset: {e}")
        raise


def process_chunk(
    df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    rule_RD: pd.DataFrame,
    rule_KN: pd.DataFrame,
    report_type: str
) -> pd.DataFrame:
    """
    Process a chunk of data: calculations, joins, and rule application.
    
    Args:
        df: DataFrame chunk to process (columns already assigned)
        lookup_df: Lookup DataFrame
        rule_RD: RD rules DataFrame
        rule_KN: KN rules DataFrame
        
    Returns:
        Processed DataFrame
    """
    try:
        use_ttkt_report = report_type == REPORT_TYPE_TTKT
        
        # Vectorized CNHUB
        df['CNHUB'] = df['don_vi_khaithac'].str[3:6]
        df.loc[df['don_vi_khaithac'] == 'HUBTAN', 'CNHUB'] = 'BDG'
        df.loc[df['don_vi_khaithac'] == 'HUBBHD', 'CNHUB'] = 'BDH'
        
        # Merge lookup once per chunk, replacing chi_nhanh_phat
        df = df.merge(lookup_df, how='left', left_on='ma_buucuc_phat', right_on='ma_buucuc')
        df['chi_nhanh_phat'] = df['ma_tinh']
        df.drop(columns=['ma_tinh', 'ma_buucuc'], inplace=True, errors='ignore')
        
        # Vectorized loai_hang depending on report type
        if use_ttkt_report:
            df['phan_loai'] = 'RD'
        else:
            df['phan_loai'] = np.where(df['CNHUB'] == df['chi_nhanh_phat'], 'RD', 'KN')
        
        # Datetime columns are already parsed in preprocessing step
        
        # Prepare result column default 'Check lại' & NaT (will be overwritten)
        df['Result_p'] = 'Check lại'
        df['deadline'] = pd.NaT
        df['timedelta'] = pd.Series(dtype='timedelta64[ns]', index=df.index)
        
        # Store original index for mapping
        df.index.name = 'orig_idx'
        
        # RD subset
        df_RD = df[df['phan_loai'] == 'RD']
        if not df_RD.empty:
            res_RD, deadline_RD, timedelta_RD = compute_result_for_subset(
                df_RD,
                rule_RD,
                left_on=['don_vi_khaithac', 'ma_buucuc_phat'],
                right_on=['don_vi_khai_thac', 'buu_cuc_phat']
            )
            df.loc[res_RD.index, 'Result_p'] = res_RD
            df.loc[deadline_RD.index, 'deadline'] = deadline_RD
            df.loc[timedelta_RD.index, 'timedelta'] = timedelta_RD
        
        # KN subset (skip for TTKT report)
        if not use_ttkt_report:
            df_KN = df[df['phan_loai'] == 'KN']
            if not df_KN.empty:
                res_KN, deadline_KN, timedelta_KN = compute_result_for_subset(
                    df_KN,
                    rule_KN,
                    left_on=['don_vi_khaithac', 'chi_nhanh_phat'],
                    right_on=['don_vi_khai_thac', 'chi_nhanh_phat']
                )
                df.loc[res_KN.index, 'Result_p'] = res_KN
                df.loc[deadline_KN.index, 'deadline'] = deadline_KN
                df.loc[timedelta_KN.index, 'timedelta'] = timedelta_KN
        
        logger.debug(f"Processed chunk with {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error processing chunk: {e}")
        raise

