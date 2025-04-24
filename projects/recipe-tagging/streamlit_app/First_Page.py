import streamlit as st
from recipe_tagging.db import get_serverless_spark_session
from streamlit_app.helpers.utils import load_json_to_df, process_data

st.set_page_config(layout="centered")

spark = get_serverless_spark_session()

# Initialize session state
if "raw_data" not in st.session_state:
    st.session_state.raw_data = None
if "processed_data" not in st.session_state:
    st.session_state.processed_data = {}
if "uploaded_filename" not in st.session_state:
    st.session_state.uploaded_filename = None
if "selected_language" not in st.session_state:
    st.session_state.selected_language = "Norwegian"

if st.session_state.raw_data is None:
    uploaded_file = st.file_uploader("Upload your recipe JSON file", type=["json"])
    if uploaded_file:
        raw_bytes = uploaded_file.read()
        raw_str = raw_bytes.decode("utf-8")
        raw_df = load_json_to_df(raw_str)

        st.session_state.raw_data = raw_df
        st.session_state.uploaded_filename = uploaded_file.name
        st.success(f"File '{uploaded_file.name}' uploaded and loaded.")
else:
    st.info(f"Using uploaded file: {st.session_state.uploaded_filename}")

selected_language = st.selectbox(
    "Select Language",
    ["Norwegian", "Swedish", "Danish"],
    index=["Norwegian", "Swedish", "Danish"].index(st.session_state.selected_language),
)
st.session_state.selected_language = selected_language

if st.session_state.raw_data is not None:
    if selected_language not in st.session_state.processed_data:
        with st.spinner(f"Processing data for {selected_language}..."):
            processed = process_data(
                st.session_state.raw_data, selected_language, spark=spark
            )
            st.session_state.processed_data[selected_language] = processed

    data = st.session_state.processed_data[selected_language]
    st.write(
        f"Loaded {data['recipe_id'].nunique()} {selected_language.lower()} recipes."
    )
    st.session_state.data = data
else:
    st.warning("Please upload a recipe file to begin.")
