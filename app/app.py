import os
import threading
import time

import streamlit as st
from streamlit.runtime.runtime import Runtime


def start_monitor():
    runtime = Runtime.instance()

    while True:
        # Count active sessions
        active = len(runtime._session_mgr.list_active_sessions())

        # If no browser tabs connected → exit Streamlit server
        if active == 0:
            os._exit(0)

        time.sleep(5)


# Start monitor only once per process
if "monitor_started" not in st.session_state:
    t = threading.Thread(target=start_monitor, daemon=True)
    t.start()
    st.session_state.monitor_started = True

st.set_page_config(page_title="VTP Streamline", page_icon="🏃‍➡️", layout="wide")


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
