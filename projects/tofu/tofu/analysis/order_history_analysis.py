# type: ignore
import numpy as np
import pandas as pd
import streamlit as st
from sklearn.linear_model import LinearRegression
from tofu.analysis.features import add_easter_features, add_holiday_features


@st.cache_data()
def prepare_data_for_mapping(
    df_known: pd.DataFrame,
    df_future: pd.DataFrame,
    country: str,
    num_weeks_around_easter: int = 3,
) -> pd.DataFrame:
    df_concat = pd.concat([df_known, df_future], ignore_index=True)
    df_concat_with_holidays = add_holiday_features(df_concat, country=country)
    df_concat_with_holidays_and_easter = add_easter_features(
        df_concat_with_holidays, num_weeks_around_easter=num_weeks_around_easter
    )

    return df_concat_with_holidays_and_easter


@st.cache_data()
def create_total_orders_mapping_table(
    df_with_holidays: pd.DataFrame,
    num_years_to_map: int,
    holidays_to_match: list[str],
) -> pd.DataFrame:
    """Create a mapping table for the total orders of the current year to the previous years."""
    available_years = np.sort(df_with_holidays["menu_year"].unique())
    df_list = []
    for current_year in available_years[num_years_to_map:]:
        year_to_map = current_year - num_years_to_map

        df_current_year = df_with_holidays[df_with_holidays["menu_year"] == current_year]
        df_year_to_map = df_with_holidays[df_with_holidays["menu_year"] == year_to_map]

        df_current_year = df_current_year.assign(mapped_menu_year=year_to_map)
        df_current_year = df_current_year.merge(
            df_year_to_map[["menu_week", "total_orders"]].rename(
                columns={
                    "total_orders": "total_orders_mapped_year_same_week",
                }
            ),
            how="left",
            on=["menu_week"],
        )

        df_year_to_map_easter = (
            df_year_to_map[df_year_to_map["is_around_easter"]][["menu_week", "num_weeks_since_easter", "total_orders"]]
            .copy()
            .rename(
                columns={
                    "total_orders": "mapped_total_orders",
                    "menu_week": "mapped_menu_week",
                }
            )
        )
        df_current_year = df_current_year.merge(
            df_year_to_map_easter,
            how="left",
            on=["num_weeks_since_easter"],
        )

        # Map current year's holidays to the previous years that are not Easter
        for holiday in holidays_to_match:
            rows_current_holiday = df_current_year.loc[df_current_year[holiday] == 1].index
            if (len(rows_current_holiday) > 0) and (
                not df_current_year.loc[rows_current_holiday[0], "mapped_menu_week"] > 0
            ):
                mapped_menu_week = df_year_to_map.loc[df_year_to_map.loc[:, holiday] == 1, "menu_week"].values
                mapped_total_orders = df_year_to_map.loc[df_year_to_map.loc[:, holiday] == 1, "total_orders"].values
                if len(mapped_menu_week) > 0:
                    df_current_year.loc[rows_current_holiday[0], "mapped_menu_week"] = mapped_menu_week
                    df_current_year.loc[rows_current_holiday[0], "mapped_total_orders"] = mapped_total_orders

        df_current_year.loc[df_current_year["mapped_menu_week"].isna(), "mapped_menu_week"] = df_current_year.loc[
            df_current_year["mapped_menu_week"].isna(), "menu_week"
        ]

        df_current_year.loc[df_current_year["mapped_total_orders"].isna(), "mapped_total_orders"] = df_current_year.loc[
            df_current_year["mapped_total_orders"].isna(), "total_orders_mapped_year_same_week"
        ]

        df_list.append(
            df_current_year[
                [
                    "menu_year",
                    "menu_week",
                    "holiday_list",
                    "is_around_easter",
                    "num_holidays",
                    "num_holidays_effectively",
                    "is_holiday_overlap_with_weekend",
                    "total_orders",
                    "flex_share",
                    "mapped_menu_year",
                    "mapped_menu_week",
                    "mapped_total_orders",
                ]
            ]
        )

    df_mapped = pd.concat(df_list, ignore_index=True)
    df_mapped["is_need_special_attention"] = False
    df_mapped = df_mapped.merge(
        df_with_holidays[["menu_year", "menu_week", "num_holidays", "num_holidays_effectively"]].rename(
            columns={
                "menu_year": "mapped_menu_year",
                "menu_week": "mapped_menu_week",
                "num_holidays": "num_holidays_mapped",
                "num_holidays_effectively": "num_holidays_effectively_mapped",
            }
        ),
        on=["mapped_menu_year", "mapped_menu_week"],
        how="left",
    )
    df_mapped.loc[
        (df_mapped["num_holidays_effectively"] != df_mapped["num_holidays_effectively_mapped"]),
        "is_need_special_attention",
    ] = True

    df_mapped = df_mapped.merge(
        df_with_holidays[["menu_year", "menu_week", "flex_share"]].rename(
            columns={
                "menu_year": "mapped_menu_year",
                "menu_week": "mapped_menu_week",
                "flex_share": "mapped_flex_share",
            }
        ),
        on=["mapped_menu_year", "mapped_menu_week"],
        how="left",
    )

    # Try mapping no holidays with a closest week that has no holidays
    zero_holiday_indices = df_mapped.loc[
        (df_mapped["is_need_special_attention"]) & (df_mapped["num_holidays_effectively"] == 0)
    ].index
    if len(zero_holiday_indices) > 0:
        for a_index in zero_holiday_indices:
            # First if last year is a better match
            menu_year, menu_week = df_mapped.loc[a_index][["menu_year", "menu_week"]].values
            df_to_check = df_mapped[(df_mapped["menu_week"] == menu_week) & (df_mapped["menu_year"] == (menu_year - 1))]
            if df_to_check.shape[0] > 0 and df_to_check["num_holidays_effectively"].values[0] == 0:
                df_mapped.loc[a_index, "mapped_menu_year"] = menu_year - 1
                df_mapped.loc[a_index, "mapped_menu_week"] = menu_week
                df_mapped.loc[a_index, "mapped_total_orders"] = df_to_check["total_orders"].values[0]
                df_mapped.loc[a_index, "mapped_flex_share"] = df_to_check["flex_share"].values[0]
                df_mapped.loc[a_index, "num_holidays_mapped"] = df_to_check["num_holidays"].values[0]
                df_mapped.loc[a_index, "num_holidays_effectively_mapped"] = df_to_check[
                    "num_holidays_effectively"
                ].values[0]
            else:
                for i in [-1, 1, -2, 2]:
                    index_to_check = a_index + i
                    if (index_to_check in df_mapped.index) and (df_mapped.loc[index_to_check, "num_holidays"] == 0):
                        mapped_menu_year, mapped_menu_week, mapped_total_orders, mapped_flex_share = df_mapped.loc[
                            index_to_check
                        ][["menu_year", "menu_week", "mapped_total_orders", "mapped_flex_share"]].values
                        df_mapped.loc[a_index, "mapped_menu_year"] = mapped_menu_year
                        df_mapped.loc[a_index, "mapped_menu_week"] = mapped_menu_week
                        df_mapped.loc[a_index, "mapped_total_orders"] = mapped_total_orders
                        df_mapped.loc[a_index, "mapped_flex_share"] = mapped_flex_share
                        break

    df_mapped = df_mapped.merge(
        df_with_holidays[["menu_year", "menu_week", "holiday_list", "is_around_easter"]].rename(
            columns={
                "menu_year": "mapped_menu_year",
                "menu_week": "mapped_menu_week",
                "is_around_easter": "is_around_easter_mapped_week",
                "holiday_list": "mapped_holiday_list",
                "is_holiday_overlap_with_weekend": "is_holiday_overlap_with_weekend_mapped",
            }
        ),
        on=["mapped_menu_year", "mapped_menu_week"],
        how="left",
    )
    # Rearrange columns such that is_need_special_attention is the first column
    df_mapped = df_mapped[
        ["is_need_special_attention"] + [col for col in df_mapped.columns if col != "is_need_special_attention"]
    ]

    return df_mapped


@st.cache_data()
def add_growth_rate_week_to_week(
    df_mapped: pd.DataFrame,
) -> pd.DataFrame:
    df_mapped["growth_rate_week_to_week"] = df_mapped["total_orders"] / df_mapped["mapped_total_orders"]
    return df_mapped


@st.cache_data()
def add_growth_rate_rolling_n_weeks(
    df_mapped: pd.DataFrame,
    n_weeks: int,
    is_exclude_holiday: bool,
) -> pd.DataFrame:
    new_col_name = f"growth_rate_rolling_{n_weeks}_weeks"

    if is_exclude_holiday:
        df_mapped["multiplier"] = ((df_mapped["num_holidays"] == 0) & (df_mapped["num_holidays_mapped"] == 0)).astype(
            int
        )
    else:
        df_mapped["multiplier"] = 1

    df_mapped["rolling_sum_total_orders"] = (
        (df_mapped["total_orders"] * df_mapped["multiplier"]).rolling(window=n_weeks).sum()
    )
    df_mapped["rolling_sum_total_orders_mapped"] = (
        (df_mapped["mapped_total_orders"] * df_mapped["multiplier"]).rolling(window=n_weeks).sum()
    )
    df_mapped[new_col_name] = df_mapped["rolling_sum_total_orders"] / df_mapped["rolling_sum_total_orders_mapped"]
    df_mapped = df_mapped.drop(columns=["multiplier"])
    return df_mapped


@st.cache_data()
def add_linear_growth_rate_projection(
    df_mapped_known: pd.DataFrame,
    df_mapped_future: pd.DataFrame,
    num_weeks_to_regress: int = 12,
    col_to_regress_on: str = "growth_rate_week_to_week",
) -> pd.DataFrame:
    y_train = df_mapped_known.tail(num_weeks_to_regress)[col_to_regress_on].values
    X_train = np.arange(len(y_train)).reshape(-1, 1)  # noqa
    model = LinearRegression()
    model.fit(X_train, y_train)
    num_weeks_to_predict = df_mapped_future.shape[0]
    X_pred = (np.arange(num_weeks_to_predict) + len(y_train)).reshape(-1, 1)  # noqa
    y_pred = model.predict(X_pred)
    new_column = f"{col_to_regress_on}_linear_projection"
    df_mapped_future[new_column] = y_pred
    return df_mapped_future


@st.cache_data()
def add_growth_projections(
    df_mapped: pd.DataFrame, df_known: pd.DataFrame, df_future: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_mapped_known = df_known[["menu_year", "menu_week"]].merge(df_mapped)
    df_mapped_future = df_future[["menu_year", "menu_week"]].merge(df_mapped)

    df_mapped_known = add_growth_rate_week_to_week(df_mapped=df_mapped_known)
    df_mapped_known = add_growth_rate_rolling_n_weeks(df_mapped=df_mapped_known, n_weeks=4, is_exclude_holiday=True)
    df_mapped_known = add_growth_rate_rolling_n_weeks(df_mapped=df_mapped_known, n_weeks=12, is_exclude_holiday=True)

    df_mapped_future["growth_rate_week_to_week"] = df_mapped_known.dropna(subset=["growth_rate_week_to_week"])[
        "growth_rate_week_to_week"
    ].values[-1]
    df_mapped_future["growth_rate_rolling_4_weeks"] = df_mapped_known.dropna(subset=["growth_rate_rolling_4_weeks"])[
        "growth_rate_rolling_4_weeks"
    ].values[-1]
    df_mapped_future["growth_rate_rolling_12_weeks"] = df_mapped_known.dropna(subset=["growth_rate_rolling_12_weeks"])[
        "growth_rate_rolling_12_weeks"
    ].values[-1]

    df_mapped_future = add_linear_growth_rate_projection(
        df_mapped_known=df_mapped_known,
        df_mapped_future=df_mapped_future,
        num_weeks_to_regress=20,
        col_to_regress_on="growth_rate_week_to_week",
    )
    return df_mapped_known, df_mapped_future


@st.cache_data()
def add_projected_orders(
    df_future_mapped: pd.DataFrame, growth_rate_columns: list[str]
) -> tuple[pd.DataFrame, list[str]]:
    projected_order_columns = []
    for growth_rate_column in growth_rate_columns:
        projected_order_column = "orders_based_on_" + growth_rate_column
        df_future_mapped[projected_order_column] = (
            df_future_mapped[growth_rate_column] * df_future_mapped["mapped_total_orders"]
        ).astype(int)
        projected_order_columns.append(projected_order_column)
    projected_order_columns.append("mapped_total_orders")
    return df_future_mapped, projected_order_columns
