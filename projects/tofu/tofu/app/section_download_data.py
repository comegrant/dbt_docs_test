import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from tofu.data import get_calendar, get_latest_forecasts, get_order_history


@st.cache_data
def section_download_data(
    company_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    load_dotenv()
    df_order_history = get_order_history(company_id=company_id)
    df_calendar = get_calendar()
    df_latest_forecasts = get_latest_forecasts(company_id=company_id)

    return df_order_history, df_calendar, df_latest_forecasts
