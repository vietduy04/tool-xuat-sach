"""Homepage"""

import os

import streamlit as st
import version

TEMP_FOLDER = ".store"

st.title("VTP Streamline")

st.info(f"Phiên bản hiện tại: {version.__version__}")
