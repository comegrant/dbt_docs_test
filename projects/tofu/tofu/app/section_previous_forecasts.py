# pyright: reportCallIssue=false

import pandas as pd
import streamlit as st


def section_previous_forecasts(
    df_latest_forecasts: pd.DataFrame,
    df_order_history: pd.DataFrame,
) -> None:
    st.header("🎯 How did the previous forecasts perform?")

    df_forecast_accuracy = df_order_history.merge(
        df_latest_forecasts,
        how="inner",
        on=["menu_year", "menu_week"],
    )
    df_forecast_accuracy["error"] = df_forecast_accuracy["forecast_total_orders"] - df_forecast_accuracy["total_orders"]

    df_forecast_accuracy["error_pct"] = df_forecast_accuracy["error"] / df_forecast_accuracy["total_orders"]
    df_forecast_accuracy["error_pct"] = df_forecast_accuracy["error_pct"].apply(lambda x: f"{x * 100:.2f}%")
    df_forecast_accuracy = df_forecast_accuracy[
        ["menu_year", "menu_week", "total_orders", "forecast_total_orders", "error", "error_pct"]
    ].rename(
        columns={
            "total_orders": "Actual",
            "forecast_total_orders": "Forecast",
            "error": "Error",
            "error_pct": "Error %",
        }
    )
    st.write(df_forecast_accuracy.tail(4).reset_index(drop=True))
