# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false

import numpy as np
import pandas as pd
import streamlit as st
from project_f.analysis.newly_established import calculate_orders_newly_established, get_newly_established_cohorts
from project_f.misc import create_date_from_year_week, override_file
from project_f.paths import DATA_DIR


@st.cache_data
def read_ne_data(brand: str) -> pd.DataFrame:
    df_weekly_decay_rate = pd.read_csv(f"{DATA_DIR}/assumptions/ne_decay_rate.csv")
    df_weekly_decay_rate = df_weekly_decay_rate[df_weekly_decay_rate["company_name"] == brand]
    return df_weekly_decay_rate


def section_newly_established(
    df_source_effect: pd.DataFrame,
    df_cohort_latest: pd.DataFrame,
    df_started_delivery: pd.DataFrame,
    df_started_delivery_merged: pd.DataFrame,
    brand: str,
    budget_end: int,
) -> tuple[pd.DataFrame, list[dict]]:
    st.header("👦🏻 Newly Established Customers")
    df_weekly_decay_rate = read_ne_data(brand=brand)
    st.subheader("💭 Assumptions: weekly decay rate")
    df_weekly_decay_rate_edited = st.data_editor(df_weekly_decay_rate, disabled=["company_name", "customer_segment"])
    if st.button("❗️Override weekly decay file with edited data"):
        file_path = DATA_DIR / "assumptions" / "ne_decay_rate.csv"
        override_file(df_new=df_weekly_decay_rate_edited, original_file_path=file_path)
        st.write(f":red[{file_path} updated!]")

    end_year = budget_end // 100
    end_week = budget_end % 100

    last_included_date = create_date_from_year_week(year=end_year, week=end_week, weekday=6)
    min_date = df_started_delivery_merged["start_delivery_date_monday"].min()
    max_established_cohort_weeks = (
        np.ceil((last_included_date - min_date).days / 7)  # or 45 as a constant from previous time
    )
    df_cohort_newly_estaliblished = get_newly_established_cohorts(
        df_cohort_latest=df_cohort_latest,
        df_weekly_decay_rate=df_weekly_decay_rate_edited,
        max_established_cohort_week=max_established_cohort_weeks,
    )
    st.subheader("👯 Newly established customers cohorts")
    st.write(df_cohort_newly_estaliblished)

    (
        df_orders_newly_established,
        df_orders_newly_established_agg,
    ) = calculate_orders_newly_established(
        df_cohort_newly_estaliblished=df_cohort_newly_estaliblished,
        df_started_delivery=df_started_delivery,
        df_source_effect=df_source_effect,
    )

    df_orders_newly_established = df_orders_newly_established[
        (df_orders_newly_established["delivery_year"] * 100 + df_orders_newly_established["delivery_week"])
        <= (end_year * 100 + end_week)
    ]
    df_orders_newly_established_agg = df_orders_newly_established_agg[
        (df_orders_newly_established_agg["delivery_year"] * 100 + df_orders_newly_established_agg["delivery_week"])
        <= (end_year * 100 + end_week)
    ]
    st.subheader("🤗:green[Results for newly established customers]")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Aggregated orders by newly established customers")
        st.dataframe(df_orders_newly_established_agg)
    with col2:
        st.subheader("Newly established customers orders break down")
        st.dataframe(df_orders_newly_established)

    data_to_save = [
        {
            "sub_folder": "newly_established",
            "file_name": "weekly_decay_rate.csv",
            "dataframe": df_weekly_decay_rate_edited,
        },
        {
            "sub_folder": "newly_established",
            "file_name": "cohort_newly_established.csv",
            "dataframe": df_cohort_newly_estaliblished,
        },
    ]
    return df_orders_newly_established_agg, data_to_save
