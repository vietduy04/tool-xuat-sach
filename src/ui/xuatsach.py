import time
import traceback
import streamlit as st
import ui.ui_components as ui
from etl.pipeline_xuatsach import pipeline_xs_hub, pipeline_xs_ttkt
from utils.io import get_folder_child


PIPELINES = {
    "xuat_sach_hub": {
        "func": pipeline_xs_hub,
        "display_name": "Xuất sạch Kho vùng tỉnh (HUB)"
    },
    "xuat_sach_ttkt": {
        "func": pipeline_xs_ttkt,
        "display_name": "Xuất sạch TTKT / LOG"
    }
}

CONFIG = ui.init_session_state()

# ----- Main page -----

st.title("Báo cáo Xuất sạch")

tab1, tab2, tab3 = st.tabs(
    ["Mô tả luồng", "Cài đặt", "Chạy xử lý"], default="Chạy xử lý"
)

with tab1:  # Mô tả luồng, yêu cầu file
    st.subheader("Mô tả")
    # st.markdown(
    #     """
    #     Kết quả xử lý:
    #         - report_date: ngày theo tên báo cáo nhập vào (nếu không nhận diện được ngày từ tên file nhập vào)
    #         - deadline

    #     """
    # )


with tab2:  # Config luồng
    st.subheader("Cài đặt chung")
    ui.synced_textbox("File tham chiếu nội tỉnh cũ (Excel)", ["common", "thamchieu_noitinh"])
    st.divider()
    
    st.markdown("### Xuất sạch HUB")

    ui.synced_textbox("Folder Rule Kết nối", ["xuat_sach_hub", "rule_kn_folder"])
    ui.synced_textbox("Folder Rule Rải đích", ["xuat_sach_hub", "rule_rd_folder"])
    ui.synced_textbox("Folder output kết quả - Kết nối", ["xuat_sach_hub", "output_rd_folder"])
    ui.synced_textbox("Folder output kết quả - Rải đích", ["xuat_sach_hub", "output_kn_folder"])
    st.divider()
    
    st.markdown("### Xuất sạch TTKT")

    ui.synced_textbox("Folder Rule LOG / TTKT", ["xuat_sach_ttkt", "rule_folder"])
    ui.synced_textbox("Folder output kết quả", ["xuat_sach_ttkt", "output_folder"])
    st.divider()

    st.markdown("### Cài đặt khác")
    st.markdown(
        """
        **Fast mode**: Đọc dữ liệu nhanh hơn, nhưng sử dụng nhiều RAM hơn.
        Có thể làm máy bị chậm, lag.
        """
    )
    ui.synced_radio("", ["True", "False"], ["pipeline_options", "fast_mode"], label_visibility="collapsed")


with tab3:  # Chạy luồng xử lý
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**1️⃣ Nhập file raw**")
        pipeline_select = ui.synced_radio(
            label="",
            options=["Xuất sạch Kho vùng tỉnh (HUB)", "Xuất sạch TTKT"],
            config_key=["pipeline_options", "pipeline_select"],
            label_visibility="collapsed",
        )
        raw_input = st.file_uploader(
                "Upload Raw Excel Files",
                type=["csv", "xlsx"],
                accept_multiple_files=True,
                key="raw_files_uploader",
                label_visibility="collapsed",
        )
    with col2:  # Tùy chọn luồng
        st.markdown("**2️⃣ Tùy chọn luồng**")
        try:
            if pipeline_select == "Xuất sạch Kho vùng tỉnh (HUB)":
                rule_rd_select = ui.synced_selectbox(
                    label="Rule rải đích",
                    options=get_folder_child(CONFIG["xuat_sach_hub"]["rule_rd_folder"], "xlsx"), # pyright: ignore[reportAttributeAccessIssue]
                    config_key=["xuat_sach_hub", "rule_rd_file"],
                )
                rule_kn_select = ui.synced_selectbox(
                    label="Rule kết nối",
                    options=get_folder_child(CONFIG["xuat_sach_hub"]["rule_kn_folder"], "xlsx"), # pyright: ignore[reportAttributeAccessIssue]
                    config_key=["xuat_sach_hub", "rule_kn_file"],
                )
            else:
                rule_kn_select = ui.synced_selectbox(
                    label="Rule LOG / TTKT",
                    options=get_folder_child(CONFIG["xuat_sach_ttkt"]["rule_folder"], "xlsx"), # type: ignore
                    config_key=["xuat_sach_ttkt", "rule_file"],
                )
        except FileNotFoundError:
            st.error("Cần setup config hợp lệ.")
    with col3:
        st.markdown("**3️⃣ Chạy xử lý**")
        if st.button("Bắt đầu xử lý", type="primary", disabled=st.session_state.processing):
            # Check conditions
            if False:
                pass
            else:
                st.session_state.processing = True
                # Get config (output, uploaded, ref files)

                # Run pipeline
                with st.spinner("Đang xử lý...", show_time=True):
                    start_time = time.time()

                    try:
                        if pipeline_select == "Xuất sạch Kho vùng tỉnh (HUB)":
                            result = pipeline_xs_hub(raw_input, CONFIG)
                        else:
                            result = pipeline_xs_ttkt(raw_input, CONFIG)
                        
                        elapsed_time = time.time() - start_time

                        # Display results
                        if result:
                            st.success("Hoàn thành")
                        # Results summary
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Số dòng raw", result["rows_in"])
                            with col2:
                                st.metric("Số dòng xử lý", result["rows_out"])
                            with col3:
                                st.metric("Thời gian xử lý", f"{elapsed_time:.2f}s")
                    
                    except Exception as e:
                        st.error(f"Error: {e}")
                        st.code(traceback.format_exc())
                    finally:
                        st.session_state.processing = False