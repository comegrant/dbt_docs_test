import uuid
from datetime import datetime

import pytz
import streamlit as st
from constants.companies import Company, get_company_by_code
from time_machine.forecasting import get_forecast_start
from tofu.data import get_forecast_start_from_db


def section_set_params() -> tuple[Company, int, int, int, str]:
    forecast_job_run_id = str(uuid.uuid4()).upper()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        company_code = st.selectbox("Select a brand", ("GL", "AMK", "LMK", "RT"))
        company = get_company_by_code(company_code)
    try:
        default_forecast_start_year, default_forecast_start_week = get_forecast_start_from_db(
            company_id=company.company_id,
        )
        (calculated_start_year, calculated_start_week) = get_forecast_start(
            cut_off_day=company.cut_off_week_day,
            forecast_date=datetime.now(tz=pytz.timezone("CET")),
        )
        if calculated_start_year != default_forecast_start_year or calculated_start_week != default_forecast_start_week:
            st.write(
                f"❗️:red[Calculated forecast start {calculated_start_year}-{calculated_start_week} differs from db!]"
            )
        else:
            st.write(f"✅:green[Calculated forecast start {calculated_start_year}-{calculated_start_week} matches db!]")

    except:  # noqa: E722
        st.write("❗️:red[No forecast start found in db, using default forecast start]")
        (default_forecast_start_year, default_forecast_start_week) = get_forecast_start(
            cut_off_day=company.cut_off_week_day,
            forecast_date=datetime.now(tz=pytz.timezone("CET")),
        )
    with col2:
        start_year = st.number_input("Forecast start year", value=default_forecast_start_year)
    with col3:
        start_week = st.number_input("Forecast start week", value=default_forecast_start_week)
    with col4:
        num_weeks = st.selectbox("Forecast horizon (weeks) ", (11, 1, 15))
    st.write(f"Run forecast for: {company.company_name}, forecast horizon = {num_weeks}")
    st.write(f"forecast job run id = {forecast_job_run_id}")

    return company, num_weeks, start_year, start_week, forecast_job_run_id
