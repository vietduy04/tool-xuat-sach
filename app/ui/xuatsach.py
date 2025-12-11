"""Streamlit UI for ETL Pipeline."""

import os
from typing import Sequence

import streamlit as st
from etl.io import save_file
from etl.pipeline_xuatsach import import_lookup, import_rule, processing_pipeline
from utils.logger import get_logger
from utils.persistence import load_config, update_config

logger = get_logger()

# Page configuration
st.set_page_config(layout="wide")

# Initialize session state
if "config_data" not in st.session_state:
    st.session_state.config_data = load_config()
if "processing" not in st.session_state:
    st.session_state.processing = False

# ------ UI components ------


def config_uploader(
    label: str,
    config_name: str,
    filetype: str | Sequence[str] | None,
) -> None:
    # Name + status
    uploaded = st.session_state.get(config_name)
    config_path = st.session_state.config_data.get(config_name)
    if not uploaded:
        if not config_path or not os.path.exists(config_path):
            st.markdown(f"**{label}**")
        else:
            st.markdown(
                f"**{label}**: "
                f":blue-background[Dùng file cũ: {os.path.basename(config_path)}]"
            )
    else:
        # Pending import and validate config, if success (return df)
        # then save to .store, if not raise exception
        # → cached when run?
        path = save_file(uploaded, persistent=True)
        st.session_state.config_data[config_name] = path
        update_config(config_name, path)
        st.markdown(f"**{label}**: :green-background[Upload thành công]")

    # Uploader
    uploader = st.file_uploader(  # noqa: F841
        label,
        type=filetype,
        key=config_name,
        label_visibility="collapsed",
        disabled=st.session_state.processing,
    )


def synced_radio(label, options, config_key, **radio_kwargs):
    # read current default from config
    current = st.session_state.config_data.get(config_key, options[0])

    # render radio
    choice = st.radio(
        label,
        options,
        index=options.index(current),
        key=f"radio_{config_key}",
        **radio_kwargs,
    )

    # update config if changed
    if choice != current:
        update_config(config_key, choice)
        st.session_state.config_data[config_key] = choice

    return choice


# Title and Description
st.title("Báo cáo Xuất sạch")

# Tabs
tab1, tab2 = st.tabs(["Giới thiệu", "Xử lý"], default="Xử lý")

with tab1:  # Tab 1: About
    st.subheader("Mô tả chung")
    st.markdown("Mô tả về luồng, cách sử dụng và yêu cầu với các file dữ liệu")

with tab2:  # Tab 2: Config + Process
    col1, col2 = st.columns([0.4, 0.6])

    with col1:
        st.subheader("⚙️ Cài đặt")

        report_type = st.radio(
            "Loại báo cáo",
            ["Xuất sạch Kho vùng tỉnh (HUB)", "Xuất sạch TTKT"],
            key="report_type_select",
        )

        st.divider()
        config_uploader(
            "Tham chiếu tỉnh thành cũ",
            "lookup",
            "xlsx",
        )
        config_uploader(
            "Rule Rải đích",
            "rule_rd",
            "xlsx",
        )
        config_uploader(
            "Rule Kết nối",
            "rule_kn",
            "xlsx",
        )

    with col2:
        st.subheader("🔄 Xử lý dữ li")

        st.markdown("**File XLSX / CSV raw**")

        raw_format = st.radio(
            "Loại file Raw",
            ["xlsx", "csv"],
            key="report_type_select",
            horizontal=True,
        )

        raw_files = st.file_uploader(
            "Upload Raw Excel Files",
            type=raw_format,
            accept_multiple_files=True,
            key="raw_files_uploader",
            label_visibility="collapsed",
        )

        output_format = synced_radio(
            "Loại file output",
            ["xlsx", "csv"],
            "report_type",
            horizontal=True,
        )

        if st.button(
            "Bắt đầu xử lý", type="primary", disabled=st.session_state.processing
        ):
            if not raw_files:
                st.error("Cần upload file raw để chạy")
            elif not st.session_state.config_data.get("lookup"):
                st.error("Cần upload file tham chiếu")
            elif not st.session_state.config_data.get("rule_rd"):
                st.error("Cần upload rule Rải đích")
            elif not st.session_state.config_data.get("rule_kn"):
                st.error("Cần upload rule Kết nối")
            else:
                processing_pipeline(raw_files, raw_format, report_type)

        st.subheader("Output")
        if st.session_state.processing:
            st.info("Processing in progress...")
        else:
            if "output_files" in st.session_state:
                st.success("Processing completed!")
                # st.metric("Total Rows Processed", st.session_state.total_rows)
                # st.write("**Output Files:**")
                # for file_path in st.session_state.output_files:
                #     st.code(file_path)
                #     if os.path.exists(file_path):
                #         file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                #         st.caption(f"Size: {file_size:.2f} MB")
