# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false

import pandas as pd
import streamlit as st
from project_f.analysis.appendix import make_quarter_summary
from project_f.paths import DATA_DOWNLOADED_DIR
from project_f.visualisation import plot_historical_budget, plot_quarter_summary


def get_previous_budget_data(brand: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_calendar = pd.read_csv(DATA_DOWNLOADED_DIR / "budget_calendar.csv")
    df_previous_budget = pd.read_csv(DATA_DOWNLOADED_DIR / "previous_budget.csv")
    df_previous_budget = df_previous_budget[df_previous_budget["company_name"] == brand]
    return df_calendar, df_previous_budget


def section_appendix(
    brand: str,
    df_summary_merged: pd.DataFrame,
    budget_start: int,
    budget_type: str,
) -> list[dict]:
    st.subheader("Total orders compared to previous projections")
    df_calendar, df_historical_budget = get_previous_budget_data(brand=brand)
    fig_hist_budget = plot_historical_budget(
        df_latest_budget=df_summary_merged,
        df_historical_budget=df_historical_budget,
        budget_start=budget_start,
        budget_type=budget_type,
    )
    st.plotly_chart(fig_hist_budget)

    st.subheader("Quarter summary")
    df_quarter_summary = make_quarter_summary(
        df_historical_budget=df_historical_budget,
        df_budget_latest=df_summary_merged,
        df_calendar=df_calendar,
        budget_type=budget_type,
        budget_start=budget_start,
    )

    col1, col2 = st.columns((0.4, 0.6))
    with col1:
        st.dataframe(df_quarter_summary)
    with col2:
        fig_quarter_summary = plot_quarter_summary(df_quarter_summary)
        st.plotly_chart(fig_quarter_summary)

    data_to_save = [
        {
            "sub_folder": "model_outputs",
            "file_name": "quarter_summary.csv",
            "dataframe": df_quarter_summary,
        },
    ]
    return data_to_save
