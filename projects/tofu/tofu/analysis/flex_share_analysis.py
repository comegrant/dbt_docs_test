import pandas as pd
import streamlit as st


@st.cache_data()
def add_flex_share_development_week_to_week(
    df_mapped: pd.DataFrame,
) -> pd.DataFrame:
    df_mapped["flex_share_development_week_to_week"] = df_mapped["flex_share"] / df_mapped["mapped_flex_share"]
    return df_mapped


@st.cache_data()
def add_flex_share_development_rolling_n_weeks(
    df_mapped: pd.DataFrame,
    n_weeks: int,
    is_exclude_holiday: bool,
) -> pd.DataFrame:
    new_col_name = f"flex_share_development_rolling_{n_weeks}_weeks"

    if is_exclude_holiday:
        df_mapped["multiplier"] = ((df_mapped["num_holidays"] == 0) & (df_mapped["num_holidays_mapped"] == 0)).astype(
            int
        )
    else:
        df_mapped["multiplier"] = 1

    df_mapped["rolling_sum_flex_share"] = (
        (df_mapped["flex_share"] * df_mapped["multiplier"]).rolling(window=n_weeks).sum()
    )
    df_mapped["rolling_sum_flex_share_mapped"] = (
        (df_mapped["mapped_flex_share"] * df_mapped["multiplier"]).rolling(window=n_weeks).sum()
    )
    df_mapped[new_col_name] = df_mapped["rolling_sum_flex_share"] / df_mapped["rolling_sum_flex_share_mapped"]
    df_mapped = df_mapped.drop(columns=["multiplier"])
    return df_mapped


@st.cache_data()
def add_flex_development_projections(
    df_mapped: pd.DataFrame, df_known: pd.DataFrame, df_future: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_mapped_known = df_known[["menu_year", "menu_week"]].merge(df_mapped)
    df_mapped_future = df_future[["menu_year", "menu_week"]].merge(df_mapped)

    df_mapped_known = add_flex_share_development_week_to_week(df_mapped=df_mapped_known)
    df_mapped_known = add_flex_share_development_rolling_n_weeks(
        df_mapped=df_mapped_known, n_weeks=4, is_exclude_holiday=True
    )
    df_mapped_known = add_flex_share_development_rolling_n_weeks(
        df_mapped=df_mapped_known, n_weeks=12, is_exclude_holiday=True
    )

    df_mapped_future["flex_share_development_week_to_week"] = df_mapped_known.dropna(
        subset=["flex_share_development_week_to_week"]
    )["flex_share_development_week_to_week"].values[-1]

    df_mapped_future["flex_share_development_rolling_4_weeks"] = df_mapped_known.dropna(
        subset=["flex_share_development_rolling_4_weeks"]
    )["flex_share_development_rolling_4_weeks"].values[-1]

    df_mapped_future["flex_share_development_rolling_12_weeks"] = df_mapped_known.dropna(
        subset=["flex_share_development_rolling_12_weeks"]
    )["flex_share_development_rolling_12_weeks"].values[-1]

    return df_mapped_known, df_mapped_future


@st.cache_data()
def add_projected_flex_share(
    df_future_mapped: pd.DataFrame, flex_share_development_columns: list[str]
) -> tuple[pd.DataFrame, list[str]]:
    projected_flex_share_columns = []
    for flex_share_dev_column in flex_share_development_columns:
        projected_flex_share_column = "projected_based_on_" + flex_share_dev_column
        df_future_mapped[projected_flex_share_column] = (
            df_future_mapped[flex_share_dev_column] * df_future_mapped["mapped_flex_share"]
        )
        projected_flex_share_columns.append(projected_flex_share_column)
    projected_flex_share_columns.append("mapped_flex_share")
    return df_future_mapped, projected_flex_share_columns
