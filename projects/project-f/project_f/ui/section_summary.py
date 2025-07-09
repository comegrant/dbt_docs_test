# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false
# pyright: reportAssignmentType=false
# pyright: reportCallIssue=false

import pandas as pd
import streamlit as st
from project_f.analysis.summary import (
    calculate_total_orders,
    make_seasonal_effect_df,
    merge_order_with_forecast,
    process_orders,
)
from project_f.misc import override_file
from project_f.paths import DATA_DIR, DATA_DOWNLOADED_DIR
from project_f.visualisation import plot_order_summary


def get_orders_forecast(
    brand: str,
    budget_start: int,
) -> pd.DataFrame:
    df_orders_forecast = pd.read_csv(DATA_DOWNLOADED_DIR / "orders_forecast.csv")
    df_orders_forecast = df_orders_forecast[
        (df_orders_forecast["company_name"] == brand)
        & ((df_orders_forecast["menu_year"] * 100 + df_orders_forecast["menu_week"]) >= budget_start)
    ][["company_name", "menu_year", "menu_week", "num_orders"]].rename(
        columns={
            "menu_year": "delivery_year",
            "menu_week": "delivery_week",
            "num_orders": "forecasted_quantity",
        }
    )
    return df_orders_forecast


def get_total_order_history(brand: str) -> pd.DataFrame:
    total_orders_file = DATA_DOWNLOADED_DIR / "total_orders_per_week.csv"
    df_total_orders = pd.read_csv(total_orders_file)
    df_total_orders = df_total_orders[df_total_orders["company_name"] == brand]
    return df_total_orders


def section_summary(
    df_non_established: pd.DataFrame,
    df_newly_established: pd.DataFrame,
    df_established: pd.DataFrame,
    brand: str,
    budget_start: int,
    budget_end: int,
) -> tuple[pd.DataFrame, list[dict]]:
    st.header("📊 :green[Summary]")
    df_summary = calculate_total_orders(
        df_non_established=df_non_established,
        df_newly_established=df_newly_established,
        df_established=df_established,
    )
    df_summary = process_orders(
        df_summary=df_summary,
        budget_start=budget_start,
        budget_end=budget_end,
    )
    df_orders_forecast = get_orders_forecast(brand=brand, budget_start=budget_start)
    st.subheader("Model outputs")
    col1, col2 = st.columns([0.3, 0.7])
    with col1:
        st.subheader("Latest forecast")
        df_orders_forecast_edited = st.data_editor(
            df_orders_forecast,
            disabled=["company_name", "delivery_year", "delivery_week"],
        )
    df_summary = merge_order_with_forecast(df_summary=df_summary, df_orders_forecast=df_orders_forecast_edited)
    with col2:
        if st.button("Replace total orders with forecasted quantity"):
            st.session_state["replaced_with_forecast"] = True
        if st.session_state["replaced_with_forecast"]:
            indices_to_replace = df_summary[df_summary["forecasted_quantity"] > 0].index
            df_summary.loc[indices_to_replace, "total_orders_all_segments"] = df_summary.loc[
                indices_to_replace, "forecasted_quantity"
            ]
            df_summary.loc[indices_to_replace, "number_of_orders"] = (
                df_summary.loc[indices_to_replace, "segment_share"]
                * df_summary.loc[indices_to_replace, "forecasted_quantity"]
            ).round()
        st.dataframe(df_summary)

    seasonal_adjustment_file = DATA_DIR / "assumptions" / "seasonal_adjustment.csv"
    if not seasonal_adjustment_file.exists():
        df_seasonal_effect = make_seasonal_effect_df(df_summary=df_summary)
    else:
        df_seasonal_effect = pd.read_csv(seasonal_adjustment_file)
        if brand not in df_seasonal_effect["company_name"].unique():
            df_seasonal_effect = make_seasonal_effect_df(df_summary=df_summary)
        else:
            df_seasonal_effect = df_seasonal_effect[df_seasonal_effect["company_name"] == brand]

    col1, col2 = st.columns((0.35, 0.65))
    with col1:
        st.markdown("Adjust for weekly effect")
        df_seasonal_effect_adjusted = st.data_editor(df_seasonal_effect, disabled=["delivery_year", "delivery_week"])
        if st.button("❗️ Override seasonal adjustment with adjusted data"):
            override_file(
                df_new=df_seasonal_effect_adjusted,
                original_file_path=seasonal_adjustment_file,
            )
            st.write(f":red[{seasonal_adjustment_file} updated!]")

    df_summary_merged = df_summary.merge(df_seasonal_effect_adjusted).drop(columns=["forecasted_quantity"])
    df_summary_merged = df_summary_merged.rename(columns={"number_of_orders": "orders_model_output"})
    df_summary_merged["number_of_orders"] = (
        df_summary_merged["orders_model_output"] * df_summary_merged["effect"]
    ).round()

    df_summary_merged["total_orders_all_segments"] = (
        df_summary_merged["total_orders_all_segments"] * df_summary_merged["effect"]
    ).round()

    with col2:
        st.markdown("Number of orders after adjustment")
        st.write(df_summary_merged)
    df_total_order_history = get_total_order_history(brand=brand)
    fig = plot_order_summary(
        df_summary_merged=df_summary_merged,
        df_total_order_history=df_total_order_history,
        brand=brand,
    )

    st.plotly_chart(fig, use_container_width=True)

    data_to_save = [
        {
            "sub_folder": "model_outputs",
            "file_name": "seasonal_adjustment.csv",
            "dataframe": df_seasonal_effect_adjusted,
        },
        {
            "sub_folder": "model_outputs",
            "file_name": "number_of_orders_per_segment.csv",
            "dataframe": df_summary_merged,
        },
    ]
    return df_summary_merged, data_to_save
