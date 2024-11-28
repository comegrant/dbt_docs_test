import streamlit as st
from app.spark_context import get_serverless_spark_session
import pandas as pd


def section_get_data_from_sql() -> pd.DataFrame:
    st.header("🧑‍💻 Start with an SQL query to get the experiment data")
    # File uploader
    if st.button("Upload an sql file"):
        st.session_state["is_upload_file"] = True

    if st.button("Write an sql query here"):
        st.session_state["is_write_query"] = True

    if st.session_state["is_upload_file"]:
        uploaded_file = st.file_uploader("Choose an sql file", type=[".sql"])
        if uploaded_file is not None:
            # Read the SQL file
            sql_query = uploaded_file.read().decode("utf-8")
            st.session_state["has_query"] = True
        else:
            st.write("Please upload an sql file.")

    if st.session_state["is_write_query"]:
        sql_query = st.text_area(label="Write an sql query here..")
        if st.button("I am finished!"):
            st.session_state["has_query"] = True

    if st.session_state["has_query"]:
        st.write("Below is your sql query:")
        st.code(sql_query)
        if st.button("Run query"):
            st.session_state["is_run_query"] = True

    if st.session_state["is_run_query"]:
        st.write("Running SQL query...")
        df = fetch_data(sql_query=sql_query)
        st.dataframe(df)
        st.session_state["has_data"] = True
        return df


@st.cache_data
def fetch_data(sql_query: str) -> pd.DataFrame:
    spark = get_serverless_spark_session()
    st.write("Below is your experiment data")
    df = spark.sql(sql_query).toPandas()
    return df
