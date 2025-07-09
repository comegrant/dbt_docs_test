# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false
# pyright: reportAssignmentType=false
# pyright: reportCallIssue=false

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from project_f.analysis.appendix import prep_df_for_historical_budget_plot


def plot_cohort(df_cohort_latest_to_plot: pd.DataFrame, brand: str) -> go.Figure:
    df_cohort_one_brand = df_cohort_latest_to_plot[df_cohort_latest_to_plot["company_name"] == brand]
    df_cohort_one_brand["color_group"] = (
        df_cohort_one_brand["customer_segment"] + "_" + "month_" + df_cohort_one_brand["month"].astype(str)
    )
    # Assuming df_cohort_latest is your DataFrame and it has columns
    # 'cohort_week', 'average_orders', 'method_code', 'month' and 'customer_segment'
    fig = px.line(
        df_cohort_one_brand,
        x="cohort_week",
        y="average_orders",
        color="color_group",
    )
    fig.update_xaxes(
        tickmode="array",  # Place ticks at the locations specified by tickvals
        tickvals=np.arange(12) + 1,  # Array of locations at which ticks should be placed
        ticktext=df_cohort_one_brand["cohort_week"].unique(),
        showgrid=True,
        showticklabels=True,
    )

    return fig


def plot_onboarding_avg_orders(df_ne_summary: pd.DataFrame) -> go.Figure:
    df_ne_summary = df_ne_summary.sort_values(by=["year", "month"])
    df_ne_summary["year_month"] = df_ne_summary["year"].astype(str) + " - " + df_ne_summary["month"].astype(str)

    df_ne_summary["segment_source"] = df_ne_summary["customer_segment"] + " - " + df_ne_summary["source"]
    fig = px.bar(
        df_ne_summary,
        x="year_month",
        y="avg_orders_8_weeks",
        color="segment_source",
        barmode="group",
        title="8-week orders per customer",
    )

    return fig


def plot_order_summary(df_summary_merged: pd.DataFrame, df_total_order_history: pd.DataFrame, brand: str) -> go.Figure:
    df_summary_to_plot = df_summary_merged[df_summary_merged["company_name"] == brand]
    df_summary_to_plot["year_week"] = (
        df_summary_to_plot["delivery_year"].astype(str) + "-" + df_summary_to_plot["delivery_week"].astype(str)
    )
    # Make this fake dataframe for x axis display
    year_week = df_summary_to_plot["year_week"].unique()
    # from 0 to num_weeks
    fake_week_numbers = np.arange(len(year_week))
    df_fake_week_number = pd.DataFrame(
        {"year_week": year_week, "week_numbers": fake_week_numbers},
    )
    df_summary_to_plot_merged = df_summary_to_plot.merge(df_fake_week_number)

    # Pretend to add 1
    df_total_order_history["menu_year"] = df_total_order_history["menu_year"] + 1
    df_total_order_history = df_total_order_history[
        (df_total_order_history["menu_year"] * 100 + df_total_order_history["menu_week"]).isin(
            (df_summary_to_plot["delivery_year"] * 100 + df_summary_to_plot["delivery_week"]).unique()
        )
    ]

    df_total_order_history["year_week"] = (
        df_total_order_history["menu_year"].astype(str) + "-" + df_total_order_history["menu_week"].astype(str)
    )

    df_total_order_history_merged = df_total_order_history.merge(df_fake_week_number)
    custom_segment_order = [
        "New Customers",
        "Activated Customers",
        "Reactivated Customers",
        "Established From New Customers",
        "Established From Activated Customers",
        "Established From Reactivated Customers",
        "Established From Reactivated Customers No Code",
        "Established Customers",
    ][::-1]
    color_map = {
        "New Customers": "rgb(139, 224, 164)",
        "Activated Customers": "rgb(255, 237, 111)",
        "Reactivated Customers": "rgb(248, 156, 116)",
        "Established From New Customers": "rgb(255, 192, 203)",
        "Established From Activated Customers": "rgb(221, 160, 221)",
        "Established From Reactivated Customers": "rgb(186, 85, 211)",
        "Established From Reactivated Customers No Code": "rgb(128, 0, 128)",
        "Established Customers": "rgb(0, 0, 139)",
    }
    fig = px.area(
        df_summary_to_plot_merged,
        x="week_numbers",
        y="number_of_orders",
        color="customer_segment",
        hover_data=["segment_share", "total_orders_all_segments"],
        category_orders={"customer_segment": custom_segment_order},  # Set custom order
        labels={
            "number_of_orders": "Number of Orders",
            "year_week": "Delivery Year-Week",
            "customer_segment": "Customer Segment",
            "segment_share": "Segment Share",
            "total_orders_all_segments": "Total",
        },
        color_discrete_map=color_map,
        title="Number of Orders by Customer Segment",
    )
    fig.add_trace(
        go.Scatter(
            x=df_total_order_history_merged["week_numbers"],
            y=df_total_order_history_merged["total_orders"],
            name="total orders last year same time",
        ),
    )
    fig.update_xaxes(
        tickmode="array",  # Place ticks at the locations specified by tickvals
        tickvals=fake_week_numbers,  # Array of locations at which ticks should be placed
        ticktext=df_summary_to_plot["year_week"].unique(),
        showgrid=True,
        showticklabels=True,
    )
    return fig


def plot_historical_budget(
    df_latest_budget: pd.DataFrame,
    df_historical_budget: pd.DataFrame,
    budget_type: str,
    budget_start: int,
) -> go.Figure:
    df_budget_to_plot, df_fake_week_numbers = prep_df_for_historical_budget_plot(
        df_latest_budget=df_latest_budget,
        df_historical_budget=df_historical_budget,
        budget_type=budget_type,
        budget_start=budget_start,
    )

    fig = px.line(
        df_budget_to_plot,
        x="week_index",
        y="number_of_orders",
        color="budget_year_type",
        template="none",
    )

    fig.update_xaxes(
        tickmode="array",  # Place ticks at the locations specified by tickvals
        tickvals=np.arange(df_fake_week_numbers.shape[0]),
        ticktext=df_budget_to_plot["year_week"].unique(),
        showgrid=True,
        showticklabels=True,
    )

    return fig


def plot_quarter_summary(df_quarter_summary: pd.DataFrame) -> go.Figure:
    df_quarter_summary = df_quarter_summary.sort_values(by="year_quarter")
    fig = px.bar(
        df_quarter_summary,
        x="year_quarter",
        y="number_of_orders",
        color="budget_year_type",
        barmode="group",
    )

    return fig
