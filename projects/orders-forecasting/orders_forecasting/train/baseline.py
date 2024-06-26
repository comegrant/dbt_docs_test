import pandas as pd

from orders_forecasting.inputs.clean_data import add_prev_year_this_week


def get_rolling_avg_projection(
    df_past: pd.DataFrame,
    df_future: pd.DataFrame,
    target_col: str,
    num_past_year: int | None = 1,
    num_rolling_weeks: int | None = 4,
) -> tuple[pd.DataFrame, str, float]:
    df_train_last_n_weeks = df_past.tail(num_rolling_weeks)
    df_train_last_n_weeks, past_col = add_prev_year_this_week(
        df_from=df_past,
        df_target=df_train_last_n_weeks,
        num_years=num_past_year,
        target_col=target_col,
    )
    rolling_avg_now = df_train_last_n_weeks[target_col]
    rolling_avg_past = df_train_last_n_weeks[past_col]
    ratios = rolling_avg_now / rolling_avg_past
    ratio = ratios.median()

    df_future, past_col2 = add_prev_year_this_week(
        df_from=df_past,
        df_target=df_future,
        num_years=num_past_year,
        target_col=target_col,
    )
    projection_colname = f"projection_ra_ratio_{num_rolling_weeks}_weeks"
    df_future[projection_colname] = df_future[past_col2] * ratio
    return df_future, projection_colname, ratio


def get_week_projection(
    df_past: pd.DataFrame,
    df_future: pd.DataFrame,
    target_col: str,
    num_past_year: int = 1,
    method: str | None = "ratio",
    is_include_prev_value: bool | None = True,
) -> tuple[pd.DataFrame, str]:
    """formula:
        xhat(year, week_i) = (
            x(year, week_now)
            * x(year-num_past_year, week_i)/(x(year-num_past_year, week_now))
        )

    Args:
        df_past (pd.DataFrame): past dataframe to project FROM
        df_future (pd.DataFrame): the dataframe to project FOR
        target_col (str): target column
        num_past_year (int, optional): Defaults to 1.

    Returns:
        pd.DataFrame: the future dataframe with the projected value
    """
    # 1. find the past year values of the same week number
    df_future, past_column = add_prev_year_this_week(
        df_from=df_past,
        df_target=df_future,
        target_col=target_col,
        num_years=num_past_year,
    )
    # 2. Find the latest order value, to use as a base to project
    x_week_now = df_past.tail(1)[target_col].values[0]

    # 3. Find the past value of the projection base,
    # so that we can calculate the ratio
    overdue_week = 53
    x_past_year_week_now, past_column2 = add_prev_year_this_week(
        df_from=df_past,
        df_target=df_past[df_past["week"] != overdue_week].copy().tail(1),
        target_col=target_col,
        num_years=num_past_year,
    )
    # past_column = f"{target_col}_prev_year_{num_past_year}"
    if method == "ratio":
        projection_ratios = (
            df_future[past_column] / x_past_year_week_now[past_column2].values
        )
        projection_colname = f"projection_week_ratio_{num_past_year}_year"
        df_future[projection_colname] = x_week_now * projection_ratios
    elif method == "diff":
        projection_diff = (
            df_future[past_column] - x_past_year_week_now[past_column2].values
        )
        projection_colname = f"projection_week_diff_{num_past_year}_year"
        df_future[projection_colname] = x_week_now + projection_diff
    if not is_include_prev_value:
        df_future = df_future.drop(columns=past_column)
    return df_future, projection_colname
