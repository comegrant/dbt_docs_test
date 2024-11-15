import streamlit as st
from streamlit_app.helpers.auth import login_ui
from streamlit_app.helpers.db import load_data

st.set_page_config(page_title="Attribute Scoring", page_icon=":tomato:")
st.title("Attribute Scoring")

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login_ui()

if st.session_state["authenticated"]:
    with st.spinner("Loading data, do not exit page."):
        recipe_data, weekly_menu_data = load_data()

    st.write(f"Welcome, {st.session_state['user_name']}!")
    st.write("Use the sidebar to navigate between the pages.")

    st.session_state["recipe_data"] = recipe_data
    st.session_state["weekly_menu_data"] = weekly_menu_data
