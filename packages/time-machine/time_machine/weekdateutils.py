from datetime import date, datetime
from typing import Optional

import pytz
from dateutil.relativedelta import MO, relativedelta


def get_date_from_year_week(
    year: int,
    week_number: int,
    weekday: Optional[int] = 1,
) -> date:
    """For a given year, iso week number and week day (1 = Monday), returns the date

    Args:
        year (int): iso_year
        week_number (int): iso_week number
        weekday (Optional[int], optional): Defaults to 1 (Monday).

    Returns:
        date: a date of the type date
    """
    cet_tz = pytz.timezone("CET")
    # a day that is always in the first week of the year
    first_day = datetime(year, 1, 4, tzinfo=cet_tz)
    first_day_of_week = first_day + relativedelta(weeks=week_number - 1, weekday=MO(-1))
    # Get the specified weekday's date
    weekday_date = first_day_of_week + relativedelta(days=weekday - 1)
    return weekday_date.date()


def has_year_week_53(year: int) -> bool:
    """check if a year has week 53

    Args:
        year (int): which year

    Returns:
        bool: True if the year has iso_week 53, False otherwise
    """
    max_week = 53
    # it is not 31 December
    # becasue it could be in week 1 the year after
    last_day_of_year = date(year, 12, 28)
    return last_day_of_year.isocalendar().week == max_week
