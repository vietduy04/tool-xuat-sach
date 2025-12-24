import streamlit as st

from etl.ingest import import_files

st.set_page_config(page_title="ETL Application", layout="wide")

xuat_sach = st.Page("ui/xuatsach.py", title="Báo cáo Xuất sạch")


pg = st.navigation(
    {
        # "": [home, settings],
        # "": [home],
        "Tool dữ liệu": [xuat_sach]
    },
    position="sidebar",
    expanded=True,
)

pg.run()