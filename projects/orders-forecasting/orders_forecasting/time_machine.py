from datetime import date, datetime, timedelta

import pytz
from dateutil.relativedelta import MO, relativedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DateType, StructField, StructType

spark = SparkSession.getActiveSession()


def has_year_week_53(year: int) -> bool:
    max_week = 53
    # it is not 31 December
    # becasue it could be in week 1 the year after
    last_day_of_year = date(year, 12, 28)
    return last_day_of_year.isocalendar()[1] == max_week


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
        timezone = pytz.timezone("CET")
        start_date = datetime.now(tz=timezone).date()
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


def get_date_from_year_week(
    year: int,
    week_number: int,
    weekday: int | None,
) -> datetime:
    cet_tz = pytz.timezone("CET")
    first_day = datetime(year, 1, 4, tzinfo=cet_tz)
    first_day_of_week = first_day + relativedelta(weeks=week_number - 1, weekday=MO(-1))
    # Get the specified weekday's date
    weekday_date = first_day_of_week + relativedelta(days=weekday - 1)
    return weekday_date.date()


def get_forecast_year_weeks(
    prediction_date: datetime,
    num_weeks: int,
    cut_off_day: int,
) -> DataFrame:
    start_year, start_week = get_forecast_start(
        cut_off_day=cut_off_day, start_date=prediction_date
    )

    first_forecast_date = get_date_from_year_week(
        year=start_year, week_number=start_week, weekday=1
    )

    # Create a list of dates
    dates = [first_forecast_date + timedelta(days=7 * i) for i in range(num_weeks)]

    # Define the schema for the DataFrame
    schema = StructType([StructField("date", DateType(), True)])

    # Convert list of dates to DataFrame
    df = spark.createDataFrame([(date,) for date in dates], schema)
    df = df.withColumn("year", f.year("date")).withColumn("week", f.weekofyear("date"))

    return df.select("year", "week")
