import streamlit as st

st.set_page_config(page_title="VTP Streamline", page_icon="🏃‍➡️", layout="wide")


xuat_sach = st.Page("app.py", title="Báo cáo Xuất sạch")

pg = st.navigation(
    {
        "Tool dữ liệu": [xuat_sach],
    }
)

pg.run()
