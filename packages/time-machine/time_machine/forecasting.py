from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import pytz

from time_machine.weekdateutils import get_date_from_year_week


def get_forecast_calendar(
    num_weeks: int,
    cut_off_day: int,
    forecast_date: Optional[date] = None,
) -> pd.DataFrame:
    """For a given date, this function generates a pandas dataframe
    - with the columns year and week
    - infer the week number where the forecast should start
    - contains num_weeks weeks

    Args:
        num_weeks (int): number of weeks in the calendar
        cut_off_day (int): the cut off week day. 1 = Monday
        forecast_date (Optional[date], optional): the forecast start week
        is calculated based on this date.

    Returns:
        pd.DataFrame: a pandas dataframe with num_weeks rows, and columns
        - year
        - week
    """
    start_year, start_week = get_forecast_start(
        cut_off_day=cut_off_day, forecast_date=forecast_date
    )
    first_forecast_date = get_date_from_year_week(
        year=start_year, week_number=start_week, weekday=1
    )
    dates = [first_forecast_date + timedelta(days=7 * i) for i in range(num_weeks)]
    df = pd.DataFrame(dates)
    df.columns = ["mondays"]
    df_forecast_calendar = pd.to_datetime(df["mondays"]).dt.isocalendar()[["year", "week"]]
    df_forecast_calendar[["year", "week"]] = df_forecast_calendar[["year", "week"]].astype("Int64")

    return df_forecast_calendar


def get_forecast_start(
    cut_off_day: int,
    forecast_date: Optional[date] = None,
) -> tuple[int, int]:
    """For a given date, calculate the first year and week of the forecast.
    For a given forecast_date
     - if the week day of forecast_date is before the cut off,
     the forecasting period should start from week t+1
     - if the week day of forecast_date is past cut off,
    the forecast period should start from week t+2

    Args:
        cut_off_day (int): the cut off week day for a brand. 1 = Monday
        forecast_date (datetime | None, optional): The date on which forecast is ran.
        Set to today if left None.
    Returns:
        tuple[int, int]: the iso year and week number.
    """
    if forecast_date is None:
        timezone = pytz.timezone("CET")
        forecast_date = datetime.now(tz=timezone).date()
    this_monday = forecast_date - timedelta(forecast_date.weekday())
    next_monday = this_monday + timedelta(days=7)
    next_next_monday = this_monday + timedelta(days=14)
    day_of_week = forecast_date.isocalendar()[2]

    # Smaller or equal because cut off is at 23:59,
    # So on the day it is still before cut off
    if day_of_week <= cut_off_day:
        # start from week t+1
        # We do not take just week number plus one in case of year changes
        forecast_start_year, forecast_start_week = next_monday.isocalendar()[:2]
    else:
        # start from week t+2
        forecast_start_year, forecast_start_week = next_next_monday.isocalendar()[:2]

    return forecast_start_year, forecast_start_week
