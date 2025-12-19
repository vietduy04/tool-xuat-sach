import os
import time
import streamlit as st

@dataclass
class Pipeline

class PipelineClass

def st_run_pipeline(pipeline, input_data, config):
    result = {}
    st.session_state.processing = True
    
    pipeline_timestamp = time.strftime("")
    try:
        validate_inputs()
        
        # Validate rules & lookup nếu có
        # Folder output
        
        ctx = {}
        
        
        if config.input_type = "Từ folder":
            raw, rows_in, ctx['report_date'] = ingest_zip(input_data, config.)
        else:
            ingesst
        
        raw = ingest_files(config)
        with st.status("Đang xử lý...")
            for i in len(pipeline):
                st.status(f"Đang xử lý...(1/3: Đọc dữ liệu)")
        
        
    except:
        pass
    finally:
        save_log(result)
        st.session_state.processing = False
        return result
