from datetime import datetime

import pandas as pd
import pytz
import streamlit as st
from databricks.connect import DatabricksSession
from streamlit_app.helpers.config import OUTPUT_TABLE, URLS
from streamlit_app.helpers.sql import attribute_scoring_query, feedback_query, weekly_menu_query


def get_spark_session() -> DatabricksSession:
    return DatabricksSession.builder.serverless().getOrCreate()


@st.cache_data(ttl="1d", show_spinner=False)
def db_to_df(query: str, _spark: DatabricksSession) -> pd.DataFrame:
    return _spark.sql(query).toPandas()


def load_data() -> None:
    """Loads and merges attribute scores and weekly menu data with image and PIM links."""
    spark = get_spark_session()

    attribute_scores_df = db_to_df(attribute_scoring_query, spark)
    weekly_menu_df = db_to_df(weekly_menu_query, spark)

    weekly_menu_df = weekly_menu_df.dropna(subset="recipe_id")
    weekly_menu_df["recipe_image_link"] = URLS["img_url"] + weekly_menu_df["recipe_photo"]
    weekly_menu_df["pim_link"] = URLS["pim_url"] + weekly_menu_df["recipe_id"].astype(str)

    merged_df = attribute_scores_df.merge(weekly_menu_df, on=["company_id", "recipe_id"], suffixes=("", "_remove"))
    merged_df = merged_df.drop([i for i in merged_df.columns if "remove" in i], axis=1)

    return merged_df, weekly_menu_df


# TODO: should this be user specific?
def fetch_existing_feedback() -> pd.Series:
    """Fetches existing feedback recipe IDs."""
    spark = get_spark_session()
    try:
        existing_feedback_df = spark.sql(feedback_query).toPandas()
        return existing_feedback_df["recipe_id"].unique()
    except Exception:
        return pd.Series(dtype=int)


def send_feedback_to_db(df: pd.DataFrame) -> None:
    """Sends processed feedback data to databricks with timestamp and user info."""
    spark = get_spark_session()

    df = df.drop_duplicates()
    df["created_at"] = datetime.now(pytz.timezone("cet")).replace(tzinfo=None)
    df["created_by"] = st.session_state["user_name"].strip().lower().replace(" ", "_")

    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("append").saveAsTable(OUTPUT_TABLE)
