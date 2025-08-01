import pandas as pd
import streamlit as st
from constants.companies import Company
from tofu.analysis.flex_share_analysis import (
    add_flex_development_projections,
    add_projected_flex_share,
)
from tofu.visualisations import plot_flex_development, plot_projected_flex_share


def section_flex_share_analysis(
    df_mapped: pd.DataFrame,
    df_known: pd.DataFrame,
    df_future: pd.DataFrame,
    df_latest_forecasts: pd.DataFrame,
    num_weeks: int,
    company: Company,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    st.header("🤸‍♂️ Flex share history analysis and development")

    df_known_mapped_with_flex, df_future_mapped_with_flex = add_flex_development_projections(
        df_mapped=df_mapped, df_known=df_known, df_future=df_future
    )

    fig_flex_development = plot_flex_development(
        df_known_mapped=df_known_mapped_with_flex,
        df_future_mapped=df_future_mapped_with_flex,
        company=company,
    )
    st.plotly_chart(fig_flex_development)

    df_future_mapped_with_flex, projected_flex_share_columns = add_projected_flex_share(
        df_future_mapped=df_future_mapped_with_flex,
        flex_share_development_columns=[
            "flex_share_development_week_to_week",
            "flex_share_development_rolling_4_weeks",
            "flex_share_development_rolling_12_weeks",
        ],
    )
    df_future_mapped_with_flex = df_future_mapped_with_flex.head(num_weeks)
    st.subheader("🔮 Forecast flex shares")

    option = st.selectbox(
        "Choose a projection to be used as the baseline",
        [*projected_flex_share_columns, "enter flex share development manually"],
    )
    if option != "enter flex share development manually":
        default_predictions = df_future_mapped_with_flex[option]
    else:
        st.subheader("Flex share development for each week")
        analysts_flex_share_development_default = st.number_input(
            "Enter a flex share development (fill all fields below)", value=1.0, key="manual_flex_share_development"
        )
        num_rows = num_weeks
        num_cols = 6
        adjusted_flex_share_development = []
        for i in range(0, num_rows, num_cols):
            cols = st.columns(num_cols)
            for j in range(num_cols):
                idx = i + j
                if idx < num_rows:
                    with cols[j]:
                        adjusted_flex_share_development.append(
                            st.number_input(
                                f"Week {df_future_mapped_with_flex['menu_week'].iloc[idx]}",
                                value=float(analysts_flex_share_development_default),
                                key=f"flex_share_development_{idx}",
                                on_change=lambda: None,
                                step=0.01,
                            )
                        )
        default_predictions = df_future_mapped_with_flex["mapped_flex_share"] * adjusted_flex_share_development
        df_future_mapped_with_flex["enter flex share development manually"] = default_predictions

    num_rows = num_weeks
    num_cols = 6
    adjusted_predictions = []
    st.subheader("Projected flex shares")
    for i in range(0, num_rows, num_cols):
        cols = st.columns(num_cols)
        for j in range(num_cols):
            idx = i + j
            if idx < num_rows:
                with cols[j]:
                    adjusted_predictions.append(
                        st.number_input(
                            f"Week {df_future_mapped_with_flex['menu_week'].iloc[idx]}",
                            value=float(default_predictions.iloc[idx]),
                            key=f"pred_flex_{idx}",
                            step=0.01,
                        )
                    )
                    menu_year, menu_week = df_future_mapped_with_flex.iloc[idx][["menu_year", "menu_week"]]
                    prev_forecast_flex_share = df_latest_forecasts.loc[
                        (df_latest_forecasts["menu_year"] == menu_year)
                        & (df_latest_forecasts["menu_week"] == menu_week)
                    ]["forecast_flex_share"].values
                    if len(prev_forecast_flex_share) > 0:
                        st.write(f"prev. forecast: {prev_forecast_flex_share[0].round(2)}")
                    else:
                        st.write("prev. forecast unavailable")
    df_future_mapped_with_flex["analysts_forecast_flex_share"] = adjusted_predictions
    num_years_history_to_plot = 3
    if st.button("Show other flex share projections"):
        fig_flex_share = plot_projected_flex_share(
            df_known_mapped=df_known_mapped_with_flex,
            df_future_mapped=df_future_mapped_with_flex,
            projected_flex_share_columns=[*projected_flex_share_columns, "analysts_forecast_flex_share"],
            company=company,
            num_years_history_to_plot=num_years_history_to_plot,
        )
        if st.button("Show only analysts adjustment and baseline flex share projections"):
            fig_flex_share = plot_projected_flex_share(
                df_known_mapped=df_known_mapped_with_flex,
                df_future_mapped=df_future_mapped_with_flex,
                projected_flex_share_columns=["analysts_forecast_flex_share", option],
                company=company,
                num_years_history_to_plot=num_years_history_to_plot,
            )
    else:
        fig_flex_share = plot_projected_flex_share(
            df_known_mapped=df_known_mapped_with_flex,
            df_future_mapped=df_future_mapped_with_flex,
            projected_flex_share_columns=["analysts_forecast_flex_share", option],
            company=company,
            num_years_history_to_plot=num_years_history_to_plot,
        )
    st.plotly_chart(fig_flex_share)
    st.divider()
    return df_future_mapped_with_flex, df_known_mapped_with_flex
