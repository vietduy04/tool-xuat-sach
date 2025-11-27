"""Homepage"""

import os

import streamlit as st

# TODO: Quit logic to save last_opened to config → Load on first-run
last_opened = "xuat_sach"

if not (st.session_state.get("first")):
    st.session_state["first"] = "init"
    st.switch_page(os.path.join("ui", f"{last_opened}.py"))

st.header("VTP Streamline")

st.page_link("ui/xuat_sach.py", label="Báo cáo Xuất sạch", icon="🛠️")
