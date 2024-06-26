import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from orders_forecasting.inputs.helpers import (
    create_date_from_year_week,
    get_iso_week_numbers,
    get_year_week_from_date,
)

logger = logging.getLogger(__name__)


def get_forecast_start(
    cut_off_day: int,
    start_date: datetime | None = None,
) -> tuple[int, int]:
    """Do some checking of if the run day has passed cut off or not.
        Include this week if it's before cut off.

    Args:
        cut_off_day (int): the day of week of cut off. i.e., 1 = monday, 2 = Tues, etc.
        start_date (Optional[datetime], optional): the day when you run.
            Set to today if None.

    Returns:
        Tuple[int, int]: year, week of the forecast start date
    """
    if start_date is None:
        start_date = datetime.now(tz=timezone.utc).date()
    this_monday = start_date - timedelta(start_date.weekday())
    next_monday = this_monday + timedelta(days=7)
    next_next_monday = this_monday + timedelta(days=14)
    day_of_week = start_date.isocalendar()[2]

    if day_of_week <= cut_off_day:
        # it's before the cut off for the next week,
        # we need the week number for next week.
        # We do not take just week number plus one in case of year changes
        forecast_start_year, forecast_start_week = next_monday.isocalendar()[:2]
    else:
        forecast_start_year, forecast_start_week = next_next_monday.isocalendar()[:2]

    return forecast_start_year, forecast_start_week


def add_prev_year_this_week(
    df_from: pd.DataFrame,
    df_target: pd.DataFrame,
    target_col: str,
    num_years: int | None = 1,
) -> tuple[pd.DataFrame, str]:
    """Given a dataframe (df_target), return the target col values
        of the same week number, num_years ago.

    Args:
        df_from (pd.DataFrame): the dataframe from
        df_target (pd.DataFrame): the df to find values FOR
        target_col (str): target columns
        num_years (Optional[int], optional): Defaults to 1.

    Returns:
        np.array: the values of the previous years
    """
    # Perturb last year
    yyyywws = df_target["year"] * 100 + df_target["week"]
    prev_yyyywws = yyyywws - 100 * num_years

    df_prev_year = df_from[
        (df_from["year"] * 100 + df_from["week"]).isin(prev_yyyywws)
    ].copy()
    df_prev_year["year"] = df_prev_year["year"] + num_years
    new_colname = f"{target_col}_prev_year_{num_years}"
    if new_colname in df_target.columns:
        df_target = df_target.drop(columns=new_colname)
    df_target = df_target.merge(
        df_prev_year[["year", "week", target_col]].rename(
            columns={target_col: new_colname},
        ),
        on=["year", "week"],
        how="left",
    )
    # NAs may appear if week 53 is present in future df
    df_target[new_colname] = df_target[new_colname].ffill()
    df_target[new_colname] = df_target[new_colname].bfill()
    return df_target, new_colname


def create_test_df(start_year: int, start_week: int, horizon: int) -> pd.DataFrame:
    df_start = pd.DataFrame(
        {
            "year": [start_year],
            "week": [start_week],
        },
    )
    df_start_date = create_date_from_year_week(
        df=df_start,
        day_of_week=1,
        date_column_name="first_day_of_week",
    )

    start_date = pd.to_datetime(df_start_date["first_day_of_week"].values[0])
    end_date = start_date + timedelta(days=7 * (horizon - 1))

    df_future = get_iso_week_numbers(start_date=start_date, end_date=end_date)
    return df_future


def get_all_missing_future_weeks(
    forecast_date: datetime,
    last_known_yyyyww: int,
    forecast_horizon: int,
    cut_off_day: int,
) -> pd.DataFrame:
    yyyyww_forecast = get_year_week_from_date(a_date=forecast_date)
    forecast_start_year, forecast_start_week = get_forecast_start(
        cut_off_day=cut_off_day,
        start_date=forecast_date,
    )
    df_test = create_test_df(
        start_year=forecast_start_year,
        start_week=forecast_start_week,
        horizon=forecast_horizon,
    )

    df_last_forecast_week = df_test[
        (df_test["year"] * 100 + df_test["week"])
        == (df_test["year"] * 100 + df_test["week"]).max()
    ]
    df_last_forecast_week = create_date_from_year_week(
        df_last_forecast_week,
        day_of_week=1,
        date_column_name="date",
    )
    if yyyyww_forecast == last_known_yyyyww:
        future_df_start_date = forecast_date + timedelta(days=7)
    elif yyyyww_forecast > last_known_yyyyww:
        future_df_start_date = forecast_date
    df_future = get_iso_week_numbers(
        start_date=future_df_start_date,
        end_date=pd.to_datetime(df_last_forecast_week["date"].values[0]),
    )
    df_future["type"] = "future"
    is_test_mask = (df_future["year"] * 100 + df_future["week"]).isin(
        df_test["year"] * 100 + df_test["week"],
    )

    df_future.loc[is_test_mask, "type"] = "test"
    return df_future
