import streamlit as st
from utils.persistence import load_config, update_config, _get_nested_value, _set_nested_value


def init_session_state():
    if "config_data" not in st.session_state:
        st.session_state.config_data = load_config()
    if "processing" not in st.session_state:
        st.session_state.processing = False
    return st.session_state.config_data


def synced_textbox(label, config_key, **textbox_kwargs):
    current = _get_nested_value(st.session_state.config_data, config_key, "")
    choice = st.text_input(label, value=current, **textbox_kwargs)

    if choice != current:
        update_config(config_key, choice)
        _set_nested_value(st.session_state.config_data, config_key, choice)

def synced_radio(label, options, config_key, **radio_kwargs):
    current = _get_nested_value(st.session_state.config_data, options[0])

    # Ensure current value exists in options
    if current not in options:
        current = options[0]
        _set_nested_value(st.session_state.config_data, config_key, current)

    choice = st.radio(label, options, index=options.index(current), **radio_kwargs)
    if choice != current:
        update_config(config_key, choice)
        _set_nested_value(st.session_state.config_data, config_key, choice)

    return choice


def synced_selectbox(label, options, config_key, **selectbox_kwargs):
    current = _get_nested_value(st.session_state.config_data, options[0])
    
    # Ensure current value exists in options
    if current not in options:
        current = options[0]
        _set_nested_value(st.session_state.config_data, config_key, current)
    
    choice = st.selectbox(
        label, options, index=options.index(current), **selectbox_kwargs
    )
    if choice != current:
        update_config(config_key, choice)
        _set_nested_value(st.session_state.config_data, config_key, choice)
    return choice


def synced_segment_control(label, options, config_key, **radio_kwargs):
    current = _get_nested_value(st.session_state.config_data, options[0])
    choice = st.segmented_control(label, options, default=current, **radio_kwargs)
    if choice != current:
        update_config(config_key, choice)
        _set_nested_value(st.session_state.config_data, config_key, choice)

    return choice


# def config_uploader(
#     label: str, config_key: str, filetype: str | list[str] | None
# ) -> None:
#     # Name + status
#     uploaded = st.session_state.get(config_key)
#     config_path = st.session_state.config_data.get(config_key)

#     if not uploaded:
#         if not config_path or not os.path.exists(config_path):
#             st.markdown(f"**{label}**")
#         else:
#             st.markdown(
#                 f"**{label}** "
#                 f":blue-background[Dùng file cũ: {os.path.basename(config_path)}]"
#             )
#     else:
#         # Pending import and validate config, if success (return df)
#         # then save to .store, if not raise exception
#         # → cached when run?
#         path = save_file(uploaded, persistent=True)
#         st.session_state.config_data[config_key] = path
#         update_config(config_key, path)
#         st.markdown(f"**{label}**: :green-background[Upload thành công]")

#     # Uploader
#     uploader = st.file_uploader(  # noqa: F841
#         label,
#         type=filetype,
#         key=config_key,
#         label_visibility="collapsed",
#         disabled=st.session_state.processing,
#     )
