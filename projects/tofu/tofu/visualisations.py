import numpy as np
import pandas as pd
import plotly.graph_objects as go
from constants.companies import Company


def plot_growth_rate(
    df_known_mapped: pd.DataFrame,
    df_future_mapped: pd.DataFrame,
    company: Company,
    num_weeks_history_to_plot: int = 52,
) -> go.Figure:
    df_known_mapped_to_plot = df_known_mapped.tail(num_weeks_history_to_plot - len(df_future_mapped))
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=(df_known_mapped_to_plot["menu_year"] * 100 + df_known_mapped_to_plot["menu_week"]).astype(str),
            y=df_known_mapped_to_plot["growth_rate_week_to_week"],
            mode="lines+markers",
            hovertext=df_known_mapped_to_plot["holiday_list"],
            name="actual: growth rate this year / last year this week",
            marker_size=df_known_mapped_to_plot["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_known_mapped_to_plot["is_holiday_overlap_with_weekend"])
                    & (df_known_mapped_to_plot["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>week-on-week growth rate: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["growth_rate_week_to_week"],
            mode="lines+markers",
            line=dict(dash="dashdot"),
            hovertext=df_future_mapped["holiday_list"],
            name="growth projection: last known week-on-week",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>week-on-week growth rate: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["growth_rate_week_to_week_linear_projection"],
            mode="lines+markers",
            line=dict(dash="dash"),
            hovertext=df_future_mapped["holiday_list"],
            name="linear growth projection",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>linearly projected growth: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["growth_rate_rolling_4_weeks"],
            mode="lines+markers",
            line=dict(dash="dot"),
            hovertext=df_future_mapped["holiday_list"],
            name="growth projection: last known rolling 4 weeks",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>rolling 4 week growth: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["growth_rate_rolling_12_weeks"],
            mode="lines+markers",
            line=dict(dash="dot"),
            hovertext=df_future_mapped["holiday_list"],
            name="growth projection: last known rolling 12 weeks",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>rolling 12 week growth: %{y} <br> holidays: %{hovertext}",
        )
    )
    fig.update_layout(
        title=f"{company.company_code}: growth rate",
        xaxis=dict(
            showgrid=True,
            showticklabels=True,
            tickformat="d",
            type="category",  # This ensures even spacing between ticks
            categoryorder="array",  # Preserve the order of categories
        ),
    )
    return fig


def plot_projected_orders(
    df_known_mapped: pd.DataFrame,
    df_future_mapped: pd.DataFrame,
    projected_order_columns: list[str],
    company: Company,
    num_years_history_to_plot: int = 3,
) -> go.Figure:
    fig_order = go.Figure()
    for a_year in df_known_mapped["menu_year"].unique()[-num_years_history_to_plot:]:
        df_known_mapped_curr = df_known_mapped[df_known_mapped["menu_year"] == a_year]
        fig_order.add_trace(
            go.Scatter(
                x=df_known_mapped_curr["menu_week"],
                y=df_known_mapped_curr["total_orders"],
                mode="lines+markers",
                marker_size=df_known_mapped_curr["num_holidays"] * 7 + 5,
                marker=dict(
                    symbol=[
                        "x" if is_overlap else "circle"
                        for is_overlap in (df_known_mapped_curr["is_holiday_overlap_with_weekend"])
                        & (df_known_mapped_curr["num_holidays_effectively"] == 0)
                    ]
                ),
                name=f"total orders {a_year} - actual",
                text=df_known_mapped_curr["holiday_list"],
                hovertemplate="week: %{x}<br>total orders: %{y}<br>holidays: %{text}",
            )
        )
    for a_year in df_future_mapped["menu_year"].unique():
        df_future_mapped_curr = df_future_mapped[df_future_mapped["menu_year"] == a_year]
        for projected_order_column in projected_order_columns:
            fig_order.add_trace(
                go.Scatter(
                    x=df_future_mapped_curr["menu_week"],
                    y=df_future_mapped_curr[projected_order_column],
                    mode="lines+markers",
                    line=dict(dash="dash"),
                    marker_size=df_future_mapped_curr["num_holidays"] * 7 + 5,
                    marker=dict(
                        symbol=[
                            "x" if is_overlap else "circle"
                            for is_overlap in (df_future_mapped_curr["is_holiday_overlap_with_weekend"])
                            & (df_future_mapped_curr["num_holidays_effectively"] == 0)
                        ]
                    ),
                    name=f"predicted: {projected_order_column} - {a_year}",
                    text=df_future_mapped_curr["holiday_list"],
                    hovertemplate="week: %{x}<br>total orders: %{y}<br>holidays: %{text}",
                )
            )
    fig_order.update_layout(
        title=f"{company.company_code}: orders and projection",
        xaxis=dict(showgrid=True, showticklabels=True, tickvals=np.arange(1, 52, 2)),
    )
    return fig_order


def plot_flex_development(
    df_known_mapped: pd.DataFrame,
    df_future_mapped: pd.DataFrame,
    company: Company,
    num_weeks_history_to_plot: int = 52,
) -> go.Figure:
    df_known_mapped_to_plot = df_known_mapped.tail(num_weeks_history_to_plot - len(df_future_mapped))
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=(df_known_mapped_to_plot["menu_year"] * 100 + df_known_mapped_to_plot["menu_week"]).astype(str),
            y=df_known_mapped_to_plot["flex_share_development_week_to_week"],
            mode="lines+markers",
            hovertext=df_known_mapped_to_plot["holiday_list"],
            name="actual: flex share this year / last year this week",
            marker_size=df_known_mapped_to_plot["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_known_mapped_to_plot["is_holiday_overlap_with_weekend"])
                    & (df_known_mapped_to_plot["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>week-on-week growth rate: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["flex_share_development_week_to_week"],
            mode="lines+markers",
            line=dict(dash="dashdot"),
            hovertext=df_future_mapped["holiday_list"],
            name="flex development: last known week-on-week",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>week-on-week growth rate: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["flex_share_development_rolling_4_weeks"],
            mode="lines+markers",
            line=dict(dash="dot"),
            hovertext=df_future_mapped["holiday_list"],
            name="flex development: last known rolling 4 weeks",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>rolling 4 week growth: %{y} <br> holidays: %{hovertext}",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=(df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).astype(str),
            y=df_future_mapped["flex_share_development_rolling_12_weeks"],
            mode="lines+markers",
            line=dict(dash="dot"),
            hovertext=df_future_mapped["holiday_list"],
            name="flex development: last known rolling 12 weeks",
            marker_size=df_future_mapped["num_holidays"] * 7 + 5,
            marker=dict(
                symbol=[
                    "x" if is_overlap else "circle"
                    for is_overlap in (df_future_mapped["is_holiday_overlap_with_weekend"])
                    & (df_future_mapped["num_holidays_effectively"] == 0)
                ]
            ),
            hovertemplate="week: %{x}<br>rolling 12 week growth: %{y} <br> holidays: %{hovertext}",
        )
    )
    fig.update_layout(
        title=f"{company.company_code}: flex share development",
        xaxis=dict(
            showgrid=True,
            showticklabels=True,
            tickformat="d",
            type="category",  # This ensures even spacing between ticks
            categoryorder="array",  # Preserve the order of categories
        ),
    )
    return fig


def plot_projected_flex_share(
    df_known_mapped: pd.DataFrame,
    df_future_mapped: pd.DataFrame,
    projected_flex_share_columns: list[str],
    company: Company,
    num_years_history_to_plot: int = 3,
) -> go.Figure:
    fig_flex_share = go.Figure()
    for a_year in df_known_mapped["menu_year"].unique()[-num_years_history_to_plot:]:
        df_known_mapped_curr = df_known_mapped[df_known_mapped["menu_year"] == a_year]
        fig_flex_share.add_trace(
            go.Scatter(
                x=df_known_mapped_curr["menu_week"],
                y=df_known_mapped_curr["flex_share"],
                mode="lines+markers",
                marker_size=df_known_mapped_curr["num_holidays"] * 7 + 5,
                marker=dict(
                    symbol=[
                        "x" if is_overlap else "circle"
                        for is_overlap in (df_known_mapped_curr["is_holiday_overlap_with_weekend"])
                        & (df_known_mapped_curr["num_holidays_effectively"] == 0)
                    ]
                ),
                name=f"flex shares {a_year} - actual",
                text=df_known_mapped_curr["holiday_list"],
                hovertemplate="week: %{x}<br>total orders: %{y}<br>holidays: %{text}",
            )
        )
    for a_year in df_future_mapped["menu_year"].unique():
        df_future_mapped_curr = df_future_mapped[df_future_mapped["menu_year"] == a_year]
        for projected_flex_share_column in projected_flex_share_columns:
            fig_flex_share.add_trace(
                go.Scatter(
                    x=df_future_mapped_curr["menu_week"],
                    y=df_future_mapped_curr[projected_flex_share_column],
                    mode="lines+markers",
                    line=dict(dash="dash"),
                    marker_size=df_future_mapped_curr["num_holidays"] * 7 + 5,
                    marker=dict(
                        symbol=[
                            "x" if is_overlap else "circle"
                            for is_overlap in (df_future_mapped_curr["is_holiday_overlap_with_weekend"])
                            & (df_future_mapped_curr["num_holidays_effectively"] == 0)
                        ]
                    ),
                    name=f"predicted: {projected_flex_share_column} - {a_year}",
                    text=df_future_mapped_curr["holiday_list"],
                    hovertemplate="week: %{x}<br>total orders: %{y}<br>holidays: %{text}",
                )
            )
    fig_flex_share.update_layout(
        title=f"{company.company_code}: flex share and projection",
        xaxis=dict(showgrid=True, showticklabels=True, tickvals=np.arange(1, 52, 2)),
    )
    return fig_flex_share
