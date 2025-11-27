"""Homepage"""

import os

import streamlit as st

# TODO: Load last-opened page from config → in Homepage logic
last_opened = "xuat_sach"

if last_opened is not None:
    st.switch_page(os.path.join("ui", f"{last_opened}.py"))

st.header("VTP Streamline")
