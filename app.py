import streamlit as st

st.set_page_config(page_title="VTP Streamline", page_icon="🏃‍➡️", layout="wide")

# TODO: Quit button in main UI

home = st.Page("ui/home.py", title="Trang chủ", default=True)
xuat_sach = st.Page("ui/xuat_sach.py", title="Báo cáo Xuất sạch")
settings = st.Page("ui/settings.py", title="Cài đặt")

pg = st.navigation(
    {
        "": [home, settings],
        "Tool dữ liệu": [xuat_sach],
    }
)

pg.run()
