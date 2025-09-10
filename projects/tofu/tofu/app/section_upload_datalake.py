# pyright: reportCallIssue=false

from datetime import datetime

import pandas as pd
import pytz
import streamlit as st
from constants.companies import Company
from tofu.data import upload_df_as_csv_to_blob


def section_upload_datalake(
    df_forecast: pd.DataFrame,
    forecast_job_run_id: str,
    forecast_horizon: int,
    company: Company,
) -> None:
    # TODO: This is a temporary function to process the forecast data for the Datalake.
    # This should be removed once forecasting is completed migrated to NDP
    company_code = company.company_code
    company_id = company.company_id
    df = process_orders_forecast_for_datalake(
        df_forecast=df_forecast,
        forecast_job_run_id=forecast_job_run_id,
        forecast_horizon=forecast_horizon,
        company_id=company_id,
    )
    st.subheader("🌊 Upload forecast file to datalake")
    st.write("This is the file to be uploaded to Datalake")
    st.write(df)
    if st.button("Upload to Datalake"):
        upload_df_as_csv_to_blob(
            df=df,
            container_name="data-science",
            blob_path="forecasting_pipelines/manual_forecast/latest",
            blob_name=f"{company_code}.csv",
        )
        st.write("Latest file uploaded to Datalake!")
        archive_timestamp = datetime.now(tz=pytz.utc).strftime("%Y-%m-%d:%H:%M:%S")
        upload_df_as_csv_to_blob(
            df=df,
            container_name="data-science",
            blob_path="forecasting_pipelines/manual_forecast/archive",
            blob_name=f"{company_code}_{archive_timestamp}.csv",
        )
        st.write("Archive file uploaded to Datalake!")
        st.success("Uploaded to Datalake")


def process_orders_forecast_for_datalake(
    df_forecast: pd.DataFrame,
    forecast_job_run_id: str,
    forecast_horizon: int,
    company_id: str,
) -> pd.DataFrame:
    df_forecast_copy = (
        df_forecast[["menu_year", "menu_week", "analysts_forecast_flex_orders", "analysts_forecast_total_orders"]]
        .copy()
        .rename(
            columns={
                "menu_year": "year",
                "menu_week": "week",
            }
        )
    )

    df_forecast_copy["analyst_preselected_orders"] = (
        df_forecast_copy["analysts_forecast_total_orders"] - df_forecast_copy["analysts_forecast_flex_orders"]
    )
    df_forecast_preselected = df_forecast_copy[["year", "week", "analyst_preselected_orders"]].rename(
        columns={"analyst_preselected_orders": "quantity"}
    )
    df_forecast_preselected["variation_id"] = "20000000-0000-0000-0000-000000000000"

    df_forecast_flex = df_forecast_copy[["year", "week", "analysts_forecast_flex_orders"]].rename(
        columns={"analysts_forecast_flex_orders": "quantity"}
    )
    df_forecast_flex["variation_id"] = "10000000-0000-0000-0000-000000000000"

    df_forecast_for_datalake = pd.concat([df_forecast_flex, df_forecast_preselected], ignore_index=True)
    df_forecast_for_datalake["company_id"] = company_id
    df_forecast_for_datalake["forecast_horizon"] = forecast_horizon
    df_forecast_for_datalake["forecast_job_run_id"] = forecast_job_run_id

    column_sequence = [
        "year",
        "week",
        "company_id",
        "variation_id",
        "quantity",
        "forecast_job_run_id",
        "forecast_horizon",
    ]
    df_forecast_for_datalake = (
        df_forecast_for_datalake[column_sequence].sort_values(by=["year", "week"]).reset_index().drop(columns=["index"])
    )
    return df_forecast_for_datalake
