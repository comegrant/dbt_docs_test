import pandas as pd
import streamlit as st


@st.cache_data()
def preprocess_data(
    df_order_history: pd.DataFrame,
    df_calendar: pd.DataFrame,
    forecast_start_year: int,
    forecast_start_week: int,
    num_weeks: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_known = df_order_history[
        df_order_history["menu_year"] * 100 + df_order_history["menu_week"]
        < (forecast_start_year * 100 + forecast_start_week)
    ]
    df_known = df_known.merge(df_calendar, on=["menu_year", "menu_week"], how="left")
    df_future = df_calendar[
        df_calendar["menu_year"] * 100 + df_calendar["menu_week"] >= forecast_start_year * 100 + forecast_start_week
    ].head(num_weeks)
    df_future["total_orders"] = None
    df_future["total_orders_with_flex"] = None
    df_future["flex_share"] = None
    df_future = df_future[df_known.columns]
    return df_known, df_future  # pyright: ignore
