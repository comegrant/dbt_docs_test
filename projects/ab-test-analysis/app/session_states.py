import streamlit as st


def init_session_states() -> None:
    if "has_query" not in st.session_state:
        st.session_state["has_query"] = False
    if "is_upload_file" not in st.session_state:
        st.session_state["is_upload_file"] = False
    if "is_write_query" not in st.session_state:
        st.session_state["is_write_query"] = False
    if "is_run_query" not in st.session_state:
        st.session_state["is_run_query"] = False
    if "has_data" not in st.session_state:
        st.session_state["has_data"] = False
    if "is_indicated_group" not in st.session_state:
        st.session_state["is_indicated_group"] = False
    if "ready_to_analyze" not in st.session_state:
        st.session_state["ready_to_analyze"] = False
