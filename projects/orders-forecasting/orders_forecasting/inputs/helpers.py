from datetime import date, datetime, timedelta, timezone

import pandas as pd


def get_year_week_from_date(a_date: datetime) -> int:
    """return any date's year week number in the form of
    year * 100 + week, or, yyyyww. For example, 202335

    Args:
        a_date (datetime): any date

    Returns:
        int: yyyyww
    """
    year = a_date.isocalendar()[0]
    week = a_date.isocalendar()[1]
    yyyyww = year * 100 + week
    return yyyyww


def create_date_from_year_week(
    df: pd.DataFrame,
    day_of_week: int | None = 1,
    year_colname: str | None = "year",
    week_colname: str | None = "week",
    date_column_name: str | None = "date",
) -> pd.DataFrame:
    # Find the Monday of the week
    df[date_column_name] = pd.to_datetime(
        (
            df[year_colname].astype(str)
            + "-W"
            + df[week_colname].astype(str)
            + f"-{day_of_week}"
        ),
        format="%G-W%V-%u",
    )

    return df


def get_iso_week_numbers(
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """given a start and end date, construct the entire calendar

    Args:
        start_date (datetime): start date of the period
        end_date (datetime): end date of the period

    Returns:
        pd.DataFrame: a df with all year and week numbers of the period
    """
    current_date = start_date
    iso_weeks = []
    iso_years = []

    if start_date > end_date:
        raise ValueError("start_date must be before end_date")

    df = pd.DataFrame({"year": iso_years, "week": iso_weeks})
    # Loop through the entire period
    while current_date <= end_date:
        iso_year = current_date.isocalendar()[0]
        iso_week = current_date.isocalendar()[1]
        iso_weeks.append(iso_week)
        iso_years.append(iso_year)
        current_date += timedelta(weeks=1)
        df = pd.DataFrame({"year": iso_years, "week": iso_weeks})

    return df


def is_year_has_week_53(year: int, week: int = 53) -> bool:
    """Return whether a year has week 53

    Args:
        year (int): the year

    Returns:
        bool: true if year has week 53
    """
    week_num = date(year=year, month=12, day=28).isocalendar()[1]
    return week_num == week


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


def get_cut_off_date(
    df: pd.DataFrame,
    cut_off_dow: int,
    year_col: str | None = "year",
    week_col: str | None = "week",
) -> pd.DataFrame:
    df = create_date_from_year_week(
        df=df,
        day_of_week=cut_off_dow,
        year_colname=year_col,
        week_colname=week_col,
        date_column_name="date",
    )
    # cut off date for yyyy-ww is the cut_off_dow of the week before ww
    df["cut_off_date"] = df["date"] - timedelta(days=7)
    df = df.drop(columns="date")
    return df


def create_future_df(start_year: int, start_week: int, horizon: int) -> pd.DataFrame:
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
