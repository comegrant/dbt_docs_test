import pandas as pd
from time_machine.calendars import get_calendar_dataframe


def test_get_calendar_dataframe() -> None:
    df = get_calendar_dataframe("2022-01-01", "2022-12-31")

    # Check if the returned object is a DataFrame
    assert isinstance(df, pd.DataFrame)

    # Check if the DataFrame has the correct columns
    expected_columns = [
        "date",
        "year",
        "week",
        "datekey",
        "day_of_week",
        "weekday_name",
        "quarter",
        "month_number",
        "month_name",
    ]
    assert df.columns.tolist() == expected_columns

    # Check if the DataFrame has the correct number of rows
    expected_rows = 365
    assert len(df) == expected_rows

    # Check if the year, week, and datekey columns correspond to the date column
    assert (df["date"].dt.isocalendar().year == df["year"]).all()
    assert (df["date"].dt.isocalendar().week == df["week"]).all()
    assert (
        df["date"].astype(str).str.replace("-", "").astype(int) == df["datekey"]
    ).all()

    # Check if the day_of_week and weekday_name columns are correct
    expected_weekday_name = [
        "Saturday",
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
    ]
    expected_day_of_week = [5, 6, 0, 1, 2, 3, 4]
    assert df["weekday_name"].unique().tolist() == expected_weekday_name
    assert df["day_of_week"].unique().tolist() == expected_day_of_week

    # Check if the quarter and month_name columns are correct
    expected_quarters = [1, 2, 3, 4]
    expected_month_names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    assert df["quarter"].unique().tolist() == expected_quarters
    assert df["month_name"].unique().tolist() == expected_month_names
