# pyright: reportArgumentType=false
# pyright: reportAttributeAccessIssue=false

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import streamlit as st
from constants.companies import Company
from tofu.analysis.postprocessing import (
    calculate_forecast_adjustments,
    prepare_forecast_orders_data,
    prepare_job_run_data,
    prepare_run_metadata,
)
from tofu.configs import forecast_group_ids, forecast_job_id, forecast_model_id
from tofu.data import append_pandas_df_to_catalog
from tofu.paths import TEST_DATA_DIR


def section_summary(
    df_forecasts: pd.DataFrame,
    df_latest_forecasts: pd.DataFrame,
    company: Company,
    forecast_job_run_id: str,
    forecast_start_year: int,
    forecast_start_week: int,
    num_weeks: int,
    is_enable_upload_to_ndp: bool = False,
) -> pd.DataFrame:
    st.header("🏁  Final step: save forecasts to NDP! [Coming 🔜 ]")
    with st.expander("**Click here to see dataframes** 👈"):
        created_at = datetime.now(pytz.utc)
        st.write(f"Dataframes to be savedfor NDP, creation timestamp = {created_at}")
        df_job_run_metadata = prepare_run_metadata(
            df_forecast=df_forecasts,
            company_id=company.company_id,
            forecast_start_year=forecast_start_year,
            forecast_start_week=forecast_start_week,
            forecast_model_id=forecast_model_id,
            forecast_group_ids=forecast_group_ids,
            forecast_job_id=forecast_job_id,
            forecast_job_run_id=forecast_job_run_id,
            forecast_horizon=num_weeks,
            created_at=created_at,
        )
        df_job_run_data = prepare_job_run_data(
            df_run_metadata=df_job_run_metadata,
        )
        df_forecast_orders = prepare_forecast_orders_data(
            df_forecast=df_forecasts,
            df_run_metadata=df_job_run_metadata,
            forecast_group_ids=forecast_group_ids,
        )
        st.write("forecast job run metadata")
        st.write(df_job_run_metadata)
        st.write("forecast job run")
        st.write(df_job_run_data)
        st.write("forecast orders")
        st.write(df_forecast_orders)

        df_forecast_adjustments = calculate_forecast_adjustments(
            df_forecast=df_forecasts,
            df_latest_forecasts=df_latest_forecasts,
        )
        if st.button(" ⬆️ Upload forecasts to NDP"):
            if is_enable_upload_to_ndp:
                st.write(" 1/3 Uploading forecast orders to NDP ...")
                append_pandas_df_to_catalog(
                    df=df_forecast_orders,
                    table_name="forecast_orders",
                    schema="forecasting",
                )
                st.write(" 2/3 Uploading run meta data to NDP ...")
                append_pandas_df_to_catalog(
                    df=df_job_run_metadata,
                    table_name="forecast_job_run_metadata",
                    schema="forecasting",
                )
                st.write("3/3 Uploading run information to NDP ...")
                append_pandas_df_to_catalog(
                    df=df_job_run_data,
                    table_name="forecast_job_runs",
                    schema="forecasting",
                )

                st.write("✅ Successfully uploaded forecasts to NDP!")
            else:
                st.write("❌ Uploading forecasts to NDP is disabled for now")

        if st.button("📀 Save orders forecast locally as a csv"):
            save_predictions_locally(
                df_forecast_orders=df_forecast_orders,
                company_code=company.company_code,
            )

    st.subheader("📩 Adjustment Slack message")
    message = make_slack_message(
        df_forecast_adjustments=df_forecast_adjustments,
        company=company,
        num_weeks=num_weeks,
    )
    st.code(message)

    st.divider()
    return df_forecasts


def make_slack_message(df_forecast_adjustments: pd.DataFrame, company: Company, num_weeks: int) -> str:
    if company.country == "Norway":
        flag = "🇳🇴"
    elif company.country == "Sweden":
        flag = "🇸🇪"
    elif company.country == "Denmark":
        flag = "🇩🇰"
    else:
        flag = ""
    message = f"""
    Good morning! ☀️
    The {company.country} {flag} {num_weeks} week forecast has been updated!
    The forecast has been adjusted by the following amount:
        {company.company_code}:"""
    threshold = 100
    if num_weeks == 1:
        threshold = 0  # Always display adjustment to one week forecast
    df_past_threshold = df_forecast_adjustments[abs(df_forecast_adjustments["difference"]) >= threshold]
    if df_past_threshold.shape[0] >= 1:
        for a_row in df_forecast_adjustments.itertuples():
            if a_row.difference > threshold:
                message += f"""
            • week {a_row.menu_week} adjusted by + {a_row.difference} orders"""
            elif a_row.difference < -threshold:
                message += f"""
            • week {a_row.menu_week} adjusted by - {abs(a_row.difference)} orders"""
    else:
        message += "Minor adjustments to all weeks"
    return message


def save_predictions_locally(
    df_forecast_orders: pd.DataFrame,
    company_code: str,
) -> None:
    if not Path.exists(TEST_DATA_DIR):
        Path.mkdir(TEST_DATA_DIR)

    start_week = df_forecast_orders.sort_values(by=["menu_year", "menu_week"]).head(1)["menu_week"].values[0]
    df_forecast_orders.to_csv(TEST_DATA_DIR / f"{company_code}_forecast_orders_{start_week}.csv", index=False)
