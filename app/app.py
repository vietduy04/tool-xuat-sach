import streamlit as st

st.set_page_config(page_title="VTP Streamline", page_icon="🏃‍➡️", layout="centered")

# TODO: Quit button in main UI

home = st.Page("ui/home.py", title="Trang chủ", default=True)
xuat_sach = st.Page("ui/xuatsach.py", title="Báo cáo Xuất sạch")

pg = st.navigation(
    {
        "": [home],
        "Tool dữ liệu": [xuat_sach],
    },
    position="sidebar",
    expanded=True,
)

pg.run()
