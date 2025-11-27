"""Streamlit UI for ETL Pipeline."""

import os
import tempfile
import time

import pandas as pd
import streamlit as st

import config
from etl.ingest import convert_excel_to_csv
from etl.output import combine_csv_files, write_chunk_to_csv
from etl.preprocess import preprocess_chunk
from etl.process import preprocess_rules, process_chunk
from etl.validate import validate_chunk
from utils.logger import get_logger
from utils.persistence import load_config, save_config, update_config

logger = get_logger()

# Page configuration

# Initialize session state
if "config_data" not in st.session_state:
    st.session_state.config_data = load_config()
if "processing" not in st.session_state:
    st.session_state.processing = False
if "progress" not in st.session_state:
    st.session_state.progress = 0
if "total_rows" not in st.session_state:
    st.session_state.total_rows = 0


def save_uploaded_file(uploaded_file, file_type: str, persistent: bool = False) -> str:
    """Save uploaded file to temporary or persistent location."""
    if persistent:
        # Save to a persistent config directory
        config_dir = os.path.join(os.getcwd(), ".config_files")
        os.makedirs(config_dir, exist_ok=True)
        file_path = os.path.join(config_dir, f"{uploaded_file.name}")
    else:
        # Save to temporary location
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, uploaded_file.name)

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path


# Title and Description
st.title("Luồng ETL Xuất sạch")
st.markdown("""
Đọc kĩ hưỡng dẫn sử dụng trước khi dùng
""")

# Tabs
tab1, tab2, tab3 = st.tabs(["About", "Config", "Process"], default="Process")

with tab1:  # About tab
    st.subheader("Mô tả chung")
    st.markdown("Mô tả về luồng, cách sử dụng và yêu cầu với các trường")

with tab2:  # Config tab
    st.subheader("⚙️ Configuration")

    st.markdown("Lookup Table")
    lookup_file = st.file_uploader(
        "Upload file Excel tham chiếu", type=["xlsx"], key="lookup_uploader"
    )
    if lookup_file is not None:
        lookup_path = save_uploaded_file(lookup_file, "lookup", persistent=True)
        st.session_state.config_data["lookup_file"] = lookup_path
        update_config("lookup_file", lookup_path)
        st.success(f"✓ Lookup file uploaded: {lookup_file.name}")
    elif st.session_state.config_data.get("lookup_file"):
        lookup_path = st.session_state.config_data["lookup_file"]
        if os.path.exists(lookup_path):
            st.info(f"Using saved file: {os.path.basename(lookup_path)}")
        else:
            st.warning("Saved lookup file not found. Please upload again.")
            st.session_state.config_data["lookup_file"] = None

    st.markdown("Rules")
    rule_rd_file = st.file_uploader(
        "Upload rule Rải đích", type=["xlsx"], key="rule_rd_uploader"
    )
    if rule_rd_file is not None:
        rule_rd_path = save_uploaded_file(rule_rd_file, "rule_rd", persistent=True)
        st.session_state.config_data["rule_rd_file"] = rule_rd_path
        update_config("rule_rd_file", rule_rd_path)
        st.success(f"✓ RD rule file uploaded: {rule_rd_file.name}")
    elif st.session_state.config_data.get("rule_rd_file"):
        rule_rd_path = st.session_state.config_data["rule_rd_file"]
        if os.path.exists(rule_rd_path):
            st.info(f"Using saved file: {os.path.basename(rule_rd_path)}")
        else:
            st.warning("Saved RD rule file not found. Please upload again.")
            st.session_state.config_data["rule_rd_file"] = None

    rule_kn_file = st.file_uploader(
        "Upload rule Kết nối", type=["xlsx"], key="rule_kn_uploader"
    )
    if rule_kn_file is not None:
        rule_kn_path = save_uploaded_file(rule_kn_file, "rule_kn", persistent=True)
        st.session_state.config_data["rule_kn_file"] = rule_kn_path
        update_config("rule_kn_file", rule_kn_path)
        st.success(f"✓ KN rule file uploaded: {rule_kn_file.name}")
    elif st.session_state.config_data.get("rule_kn_file"):
        rule_kn_path = st.session_state.config_data["rule_kn_file"]
        if os.path.exists(rule_kn_path):
            st.info(f"Using saved file: {os.path.basename(rule_kn_path)}")
        else:
            st.warning("Saved KN rule file not found. Please upload again.")
            st.session_state.config_data["rule_kn_file"] = None

    # Advanced Configuration
    with st.expander("🔧 Advanced Configuration"):
        output_folder = st.text_input(
            "Output Folder",
            value=st.session_state.config_data.get(
                "output_folder", config.DEFAULT_OUTPUT_FOLDER
            ),
            key="output_folder_input",
        )
        if output_folder != st.session_state.config_data.get("output_folder"):
            st.session_state.config_data["output_folder"] = output_folder
            update_config("output_folder", output_folder)

        chunk_size = st.number_input(
            "Chunk Size",
            min_value=1000,
            max_value=1000000,
            value=st.session_state.config_data.get(
                "chunk_size", config.DEFAULT_CHUNK_SIZE
            ),
            step=10000,
            key="chunk_size_input",
        )
        if chunk_size != st.session_state.config_data.get("chunk_size"):
            st.session_state.config_data["chunk_size"] = int(chunk_size)
            update_config("chunk_size", int(chunk_size))

with tab3:  # Process tab
    st.subheader("🔄 Processing")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Raw Files")
        raw_files = st.file_uploader(
            "Upload Raw Excel Files",
            type=["xlsx"],
            accept_multiple_files=True,
            key="raw_files_uploader",
        )
        report_type_options = ["Báo cáo XS kho vùng tỉnh", "Báo cáo XS TTKT"]
        report_type = st.selectbox(
            "Loại báo cáo",
            options=report_type_options,
            index=0,
            key="report_type_select",
        )
        st.session_state.report_type = report_type

        separate_files = st.checkbox(
            "Output to separate files for each rule type",
            value=True,
            key="separate_files_checkbox",
        )

        if st.button(
            "🚀 Process Files", type="primary", disabled=st.session_state.processing
        ):
            if not raw_files:
                st.error("Cần upload file raw để chạy")
            elif not st.session_state.config_data.get("lookup_file"):
                st.error("Cần upload file tham chiếu")
            elif not st.session_state.config_data.get("rule_rd_file"):
                st.error("Cần upload rule Rải đích")
            elif not st.session_state.config_data.get("rule_kn_file"):
                st.error("Cần upload rule Kết nối")
            else:
                process_files(raw_files, separate_files, report_type)

    with col2:
        st.subheader("Output")
        if st.session_state.processing:
            st.info("Processing in progress...")
            progress_bar = st.progress(st.session_state.progress)
            st.metric("Total Rows Processed", st.session_state.total_rows)
        else:
            if "output_files" in st.session_state:
                st.success("Processing completed!")
                st.metric("Total Rows Processed", st.session_state.total_rows)
                st.write("**Output Files:**")
                for file_path in st.session_state.output_files:
                    st.code(file_path)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                        st.caption(f"Size: {file_size:.2f} MB")

st.divider()


def process_files(raw_files, separate_files: bool, report_type: str):
    """Process uploaded files through the ETL pipeline."""
    st.session_state.processing = True
    st.session_state.progress = 0
    st.session_state.total_rows = 0
    st.session_state.output_files = []

    try:
        # Create output directory
        output_folder = st.session_state.config_data.get(
            "output_folder", config.DEFAULT_OUTPUT_FOLDER
        )
        os.makedirs(output_folder, exist_ok=True)

        # Load lookup and rules
        with st.status("Loading configuration files...") as status:
            lookup_path = st.session_state.config_data["lookup_file"]
            rule_rd_path = st.session_state.config_data["rule_rd_file"]
            rule_kn_path = st.session_state.config_data["rule_kn_file"]

            lookup_df = pd.read_excel(lookup_path, engine="calamine")
            rule_RD = pd.read_excel(rule_rd_path, engine="calamine")
            rule_KN = pd.read_excel(rule_kn_path, engine="calamine")

            rule_RD = preprocess_rules(rule_RD)
            rule_KN = preprocess_rules(rule_KN)
            status.update(label="Configuration loaded", state="complete")

        # Generate output file names
        run_timestamp = time.strftime("%H%M_%d%m%Y")
        chunk_size = st.session_state.config_data.get(
            "chunk_size", config.DEFAULT_CHUNK_SIZE
        )

        if separate_files:
            rd_output_file = os.path.join(output_folder, f"RD_{run_timestamp}.csv")
            kn_output_file = os.path.join(output_folder, f"KN_{run_timestamp}.csv")
        else:
            combined_output_file = os.path.join(
                output_folder, f"Combined_{run_timestamp}.csv"
            )
            rd_output_file = os.path.join(output_folder, f"RD_temp_{run_timestamp}.csv")
            kn_output_file = os.path.join(output_folder, f"KN_temp_{run_timestamp}.csv")

        # Process each file
        total_files = len(raw_files)
        for file_idx, raw_file in enumerate(raw_files):
            with st.status(
                f"Processing file {file_idx + 1}/{total_files}: {raw_file.name}..."
            ) as status:
                # Save uploaded file temporarily
                temp_excel_path = save_uploaded_file(raw_file, "raw")

                # Ingest
                status.update(label=f"Ingesting {raw_file.name}...")
                csv_path = convert_excel_to_csv(temp_excel_path)

                # Process in chunks
                first_write = file_idx == 0
                chunk_count = 0

                for chunk in pd.read_csv(csv_path, chunksize=chunk_size, header=None):
                    # Preprocess (assigns columns and standardizes datetime)
                    chunk = preprocess_chunk(chunk, config.COLS_TO_KEEP)

                    # Process
                    chunk = process_chunk(
                        chunk, lookup_df, rule_RD, rule_KN, report_type
                    )

                    # Validate (placeholder)
                    validate_chunk(chunk)

                    # Output
                    write_chunk_to_csv(
                        chunk, rd_output_file, kn_output_file, write_header=first_write
                    )

                    if first_write:
                        first_write = False

                    chunk_count += 1
                    st.session_state.total_rows += len(chunk)
                    st.session_state.progress = min(
                        (file_idx / total_files)
                        + (
                            (chunk_count * chunk_size) / (total_files * chunk_size * 10)
                        ),
                        1.0,
                    )

                # Clean up temp files
                try:
                    os.remove(temp_excel_path)
                    os.remove(csv_path)
                except:
                    pass

                status.update(label=f"Completed {raw_file.name}", state="complete")

        # Combine files if needed
        if not separate_files:
            with st.status("Combining output files...") as status:
                combine_csv_files(rd_output_file, kn_output_file, combined_output_file)
                # Remove temporary files
                try:
                    if os.path.exists(rd_output_file):
                        os.remove(rd_output_file)
                    if os.path.exists(kn_output_file):
                        os.remove(kn_output_file)
                except:
                    pass
                st.session_state.output_files = [combined_output_file]
                status.update(label="Files combined", state="complete")
        else:
            st.session_state.output_files = [rd_output_file, kn_output_file]

        st.session_state.progress = 1.0
        st.success(
            f"✅ Processing completed! Processed {st.session_state.total_rows} total rows."
        )

    except Exception as e:
        logger.error(f"Error processing files: {e}")
        st.error(f"❌ Error during processing: {str(e)}")
        import traceback

        st.code(traceback.format_exc())
    finally:
        st.session_state.processing = False
