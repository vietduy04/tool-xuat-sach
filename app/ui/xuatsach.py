import os

import streamlit as st
from etl.pipeline_common import st_run_pipeline

from etl.pipeline_xuatsach import PIPELINE_HUB, PIPEPLINE_TTKT
from utils.io import get_folder_child

import app.ui.ui_components as ui

# from utils.logger import get_logger


# logger = get_logger()
CONFIG = ui.init_session_state()

pipeline_mapping = {
    "Xuất sạch Kho vùng tỉnh (HUB)": PIPELINE_HUB,
    "Xuất sạch TTKT": PIPEPLINE_TTKT,
}

# ----- Main page -----

st.title("Báo cáo Xuất sạch")

tab1, tab2, tab3 = st.tabs(
    ["Mô tả luồng", "Cài đặt", "Chạy xử lý"], default="Chạy xử lý"
)

with tab1:  # Mô tả luồng, yêu cầu file
    st.markdown("**Mô tả chung**: Tạo báo cáo xuất sạch Kho vùng tỉnh / ")
    st.markdown(
        """
        Kết quả xử lý:
            - report_date: ngày theo tên báo cáo nhập vào (nếu không nhận diện được ngày từ tên file nhập vào)
            - deadline

        """
    )

with tab2:  # Config luồng
    # Note: lưu config liên tục
    # Rule, lookup, input_folder, input_format (zip/folder)
    st.markdown("### Cài đặt chung")
    st.divider
    st.subheader("Xuất sạch HUB")

    ui.synced_textbox("Folder Rule Kết nối", "rule_kn_folder")
    ui.synced_textbox("Folder Rule Rải đích", "rule_rd_folder")
    ui.synced_textbox("File tham chiếu nội tỉnh cũ (Excel)", "lookup_file")

    ui.synced_textbox("Folder chứa Raw đầu vào", "raw_HUB_folder")
    ui.synced_textbox("Folder output kết quả - Kết nối", "kn_output_folder")
    ui.synced_textbox("Folder output kết quả - Rải đích", "rd_output_folder")

    st.divider
    st.subheader("Xuất sạch TTKT")

    ui.synced_textbox("Folder Rule LOG / TTKT", "rule_ttkt_folder")
    ui.synced_textbox("Folder chứa Raw đầu vào", "raw_TTKT_folder")
    ui.synced_textbox("Folder output kết quả", "output_folder_ttkt")

    st.divider
    st.subheader("Cài đặt khác")
    st.markdown(
        """
        **Fast mode**: Đọc dữ liệu nhanh hơn, nhưng sử dụng nhiều RAM hơn.
        Có thể làm máy bị chậm, lag.
        """
    )
    ui.synced_radio("", [True, False], "fast_mode", label_visibility="collapsed")

with tab3:  # Chạy luồng xử lý
    col1, col2, col3 = st.columns(3)

    with col1:  # Input
        st.markdown("**Nhập file raw**")
        input_type = ui.synced_segment_control(
            label="",
            options=["Từ folder", "Upload thủ công"],
            config_key="input_type",
            label_visibility="collapsed",
            width="stretch",
        )

        if input_type == "Từ folder":
            input_mapping = {
                "Xuất sạch Kho vùng tỉnh (HUB)": "raw_HUB_folder",
                "Xuất sạch TTKT": "raw_TTKT_folder",
            }
            raw_input = st.selectbox(
                label="",
                options=get_folder_child(input_mapping[CONFIG.pipeline_select], "zip"),
                label_visibility="collapsed",
                width="stretch",
            )

        else:
            raw_input = st.file_uploader(
                "Upload Raw Excel Files",
                type=["csv", "xlsx"],
                accept_multiple_files=True,
                key="raw_files_uploader",
                label_visibility="collapsed",
            )
    with col2:  # Tùy chọn luồng
        st.markdown("**Loại báo cáo**")

        pipeline_select = ui.synced_radio(
            label="",
            options=["Xuất sạch Kho vùng tỉnh (HUB)", "Xuất sạch TTKT"],
            config_key="pipeline_select",
            label_visibility="collapsed",
        )

        if pipeline_select == "Xuất sạch Kho vùng tỉnh (HUB)":
            st.markdown("**Rule rải đích**")
            rule_rd_select = ui.synced_selectbox(
                label="",
                options=get_folder_child(CONFIG.rule_rd_folder, "xlsx"),
                config_key="rule_rd",
                label_visibility="collapsed",
            )
            st.markdown("**Rule kết nối**")
            rule_kn_select = ui.synced_selectbox(
                label="",
                options=get_folder_child(CONFIG.rule_kn_folder, "xlsx"),
                config_key="rule_kn",
                label_visibility="collapsed",
            )
        else:
            st.markdown("**Rule LOG / TTKT**")
            rule_kn_select = ui.synced_selectbox(
                label="",
                options=get_folder_child(CONFIG.rule_ttkt_folder, "xlsx"),
                config_key="rule_ttkt",
                label_visibility="collapsed",
            )

    with col3:
        if st.button(
            "Bắt đầu xử lý", type="primary", disabled=st.session_state.processing
        ):
            st_run_pipeline(
                pipeline=pipeline_mapping[pipeline_select],  # pyright: ignore[reportArgumentType]
                input_data=raw_input,
                config=CONFIG,
            )
