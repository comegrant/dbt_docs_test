import pandas as pd
import streamlit as st
from constants.companies import Company
from tofu.analysis.estimations_analysis import (
    get_estimations_delta_table,
    get_estimations_pivot_table,
    get_latest_estimations,
)
from tofu.analysis.order_history_analysis import (
    add_growth_projections,
    add_projected_orders,
    create_total_orders_mapping_table,
    prepare_data_for_mapping,
)
from tofu.analysis.preprocessing import preprocess_data
from tofu.configs import holidays_to_match_dict
from tofu.visualisations import plot_growth_rate, plot_projected_orders


def section_order_history_analysis(
    df_order_history: pd.DataFrame,
    df_calendar: pd.DataFrame,
    df_latest_forecasts: pd.DataFrame,
    df_estimations: pd.DataFrame,
    forecast_start_year: int,
    forecast_start_week: int,
    num_weeks: int,
    company: Company,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    st.header("📈 Order History Analysis and Growth Projections")

    df_known, df_future = preprocess_data(
        df_order_history=df_order_history,
        df_calendar=df_calendar,
        forecast_start_year=forecast_start_year,
        forecast_start_week=forecast_start_week,
        num_weeks=num_weeks
        + 2,  # we need to add 2 weeks to so that we can map the holidays which happen at the last forecast week
    )
    df_concat_with_holidays = prepare_data_for_mapping(
        df_known=df_known, df_future=df_future, country=company.country, num_weeks_around_easter=3
    )
    holiday_to_match_list = holidays_to_match_dict[company.country]
    df_mapped = create_total_orders_mapping_table(
        df_with_holidays=df_concat_with_holidays, num_years_to_map=1, holidays_to_match=holiday_to_match_list
    )

    df_known_mapped, df_future_mapped = add_growth_projections(
        df_mapped=df_mapped, df_known=df_known, df_future=df_future
    )

    fig = plot_growth_rate(
        df_known_mapped=df_known_mapped,
        df_future_mapped=df_future_mapped,
        company=company,
    )
    st.plotly_chart(fig)

    df_future_mapped, projected_order_columns = add_projected_orders(
        df_future_mapped=df_future_mapped,
        growth_rate_columns=[
            "growth_rate_week_to_week",
            "growth_rate_rolling_4_weeks",
            "growth_rate_rolling_12_weeks",
            "growth_rate_week_to_week_linear_projection",
        ],
    )
    df_future_mapped = df_future_mapped.head(num_weeks)
    st.subheader("🔮 Make Forecasts")

    option = st.selectbox(
        "Choose a projection to be used as the baseline",
        [*projected_order_columns, "enter manually"],
    )
    if option != "enter manually":
        default_predictions = df_future_mapped[option]
    else:
        st.subheader("Growth rate for each week")
        analysts_growth_rate_default = st.number_input(
            "Enter a growth rate (fill all fields below)", value=1.0, key="manual_prediction"
        )
        num_rows = num_weeks
        num_cols = 6
        adjusted_growth_rates = []
        for i in range(0, num_rows, num_cols):
            cols = st.columns(num_cols)
            for j in range(num_cols):
                idx = i + j
                if idx < num_rows:
                    with cols[j]:
                        adjusted_growth_rates.append(
                            st.number_input(
                                f"Week {df_future_mapped['menu_week'].iloc[idx]}",
                                value=float(analysts_growth_rate_default),
                                key=f"growth_rate_{idx}",
                                on_change=lambda: None,
                                step=0.01,
                            )
                        )
        default_predictions = df_future_mapped["mapped_total_orders"] * adjusted_growth_rates
        df_future_mapped["enter manually"] = default_predictions

    num_rows = num_weeks
    num_cols = 6
    adjusted_predictions = []
    st.subheader("Projected Orders")
    for i in range(0, num_rows, num_cols):
        cols = st.columns(num_cols)
        for j in range(num_cols):
            idx = i + j
            if idx < num_rows:
                with cols[j]:
                    adjusted_predictions.append(
                        st.number_input(
                            f"Week {df_future_mapped['menu_week'].iloc[idx]}",
                            value=int(default_predictions.iloc[idx]),
                            key=f"pred_{idx}",
                            on_change=lambda: None,
                            step=100,
                        )
                    )
                    menu_year, menu_week = df_future_mapped.iloc[idx][["menu_year", "menu_week"]]
                    prev_forecast_total_orders = df_latest_forecasts.loc[
                        (df_latest_forecasts["menu_year"] == menu_year)
                        & (df_latest_forecasts["menu_week"] == menu_week)
                    ]["forecast_total_orders"].values
                    if len(prev_forecast_total_orders) > 0:
                        st.write(f"prev. forecast: {prev_forecast_total_orders[0]}")
                    else:
                        st.write("prev. forecast unavailable")

    df_future_mapped["analysts_forecast_total_orders"] = adjusted_predictions
    num_years_history_to_plot = 3
    if st.button("Show other projections"):
        fig_order = plot_projected_orders(
            df_known_mapped=df_known_mapped,
            df_future_mapped=df_future_mapped,
            projected_order_columns=[*projected_order_columns, "analysts_forecast_total_orders"],
            company=company,
            num_years_history_to_plot=num_years_history_to_plot,
        )
        if st.button("Show only analysts adjustment and baseline projections"):
            fig_order = plot_projected_orders(
                df_known_mapped=df_known_mapped,
                df_future_mapped=df_future_mapped,
                projected_order_columns=["analysts_forecast_total_orders", option],
                company=company,
                num_years_history_to_plot=num_years_history_to_plot,
            )
    else:
        fig_order = plot_projected_orders(
            df_known_mapped=df_known_mapped,
            df_future_mapped=df_future_mapped,
            projected_order_columns=["analysts_forecast_total_orders", option],
            company=company,
            num_years_history_to_plot=num_years_history_to_plot,
        )
    st.plotly_chart(fig_order)
    st.subheader("💡 mapping table for sanity check")
    st.write(df_future_mapped.drop(columns=["total_orders", "flex_share"]))
    st.divider()

    st.subheader("Additional information from active basket estimations")
    with st.expander("**Click here** 👈"):
        st.write("**Latest estimations for total orders**")
        df_latest_estimations = get_latest_estimations(
            df_estimations=df_estimations,
            df_future_mapped=df_future_mapped,
        )
        st.write(df_latest_estimations)

        df_estimations_pivoted, weeks_to_include = get_estimations_pivot_table(
            df_estimations=df_estimations,
            df_future_mapped=df_future_mapped,
            df_known_mapped=df_known_mapped,
            num_weeks_known_to_include=10,
        )
        st.write("**Active basket development**")
        st.write(df_estimations_pivoted)

        st.write("**Changes in active baskets between days before cutoff (x-y)**")
        df_estimations_delta = get_estimations_delta_table(
            df_estimations=df_estimations,
            df_known_mapped=df_known_mapped,
            df_latest_estimations=df_latest_estimations,
            weeks_to_include=weeks_to_include,
        )
        st.write(df_estimations_delta)

    return df_mapped, df_known_mapped, df_future_mapped, df_concat_with_holidays
