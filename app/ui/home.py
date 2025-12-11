"""Homepage"""

import os

import streamlit as st
import version

# TODO: Quit logic to save last_opened to config → Load on first-run
last_opened = "xuatsach"

if not (st.session_state.get("resume")):
    st.session_state["resume"] = True
    st.switch_page(os.path.join("ui", f"{last_opened}.py"))

st.title("VTP Streamline")

st.info(f"Phiên bản hiện tại: {version.__version__}")
