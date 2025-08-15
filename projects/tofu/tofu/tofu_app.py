import asyncio

import streamlit as st

from tofu.app.section_appendix import section_appendix
from tofu.app.section_download_data import section_download_data
from tofu.app.section_flex_share_analysis import section_flex_share_analysis
from tofu.app.section_order_history_analysis import section_order_history_analysis
from tofu.app.section_params import section_set_params
from tofu.app.section_summary import section_summary

st.set_page_config(
    layout="wide",
    page_title="TOFU",
    page_icon="🦄",
)


async def main() -> None:
    st.title(" 🦄 TOFU: Total Order Forecast Unicorn")
    st.subheader("🪄 It's magical. It's mysterious. It does all sorts of things")
    st.subheader("😬 Although it does weird things sometimes.")
    st.divider()

    company, num_weeks, forecast_start_year, forecast_start_week, forecast_job_run_id = section_set_params()
    df_order_history, df_calendar, df_latest_forecasts, df_estimations = section_download_data(
        company_id=company.company_id
    )
    st.divider()
    df_mapped, df_known, df_future, df_concat_with_holidays = section_order_history_analysis(
        df_order_history=df_order_history,
        df_calendar=df_calendar,
        df_latest_forecasts=df_latest_forecasts,
        df_estimations=df_estimations,
        forecast_start_year=forecast_start_year,
        forecast_start_week=forecast_start_week,
        num_weeks=num_weeks,
        company=company,
    )

    df_future_mapped_with_flex, df_known_mapped_with_flex = section_flex_share_analysis(
        df_mapped=df_mapped,
        df_known=df_known,
        df_future=df_future,
        df_latest_forecasts=df_latest_forecasts,
        num_weeks=num_weeks,
        company=company,
    )
    section_summary(
        df_future_mapped=df_future,
        df_future_mapped_with_flex=df_future_mapped_with_flex,
        company=company,
        forecast_job_run_id=forecast_job_run_id,
        forecast_start_year=forecast_start_year,
        forecast_start_week=forecast_start_week,
        df_latest_forecasts=df_latest_forecasts,
        num_weeks=num_weeks,
    )

    section_appendix(
        df_future_mapped_with_flex=df_future_mapped_with_flex,
        df_known_mapped_with_flex=df_known_mapped_with_flex,
    )


if __name__ == "__main__":
    asyncio.run(main())
