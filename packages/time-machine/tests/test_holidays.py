import holidays
import pandas as pd
import pytest
from time_machine.holidays import (
    get_calendar_dataframe_with_holiday_features,
    get_holidays_dataframe,
    get_holidays_dictionary,
    get_weekly_calendar_with_holiday_features,
)


def test_get_holidays_dictionary() -> None:
    # Valid inputs
    start_date = "2022-01-01"
    end_date = "2023-12-31"
    country = "Norway"

    expected_holidays = holidays.Norway(years=[2022, 2023])
    result = get_holidays_dictionary(start_date, end_date, country)
    assert result == expected_holidays

    # Invalid date range (end_date before start_date)
    start_date = "2023-01-01"
    end_date = "2022-12-31"
    country = "Sweden"
    with pytest.raises(ValueError):  # noqa
        get_holidays_dictionary(start_date, end_date, country)

    # Valid date range and unsupported country
    start_date = "2025-01-01"
    end_date = "2025-12-31"
    country = "Germany"
    with pytest.raises(ValueError):  # noqa
        get_holidays_dictionary(start_date, end_date, country)


def test_get_holidays_dataframe() -> None:
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    country = "Sweden"

    expected_columns = ["date", "year", "week", "holiday"]

    df = get_holidays_dataframe(start_date, end_date, country)

    assert isinstance(df, pd.DataFrame)
    assert set(df.columns.tolist()) == set(expected_columns)
    assert df["date"].dt.year.min() == pd.to_datetime(start_date).year
    assert df["date"].dt.year.max() == pd.to_datetime(end_date).year
    assert df["holiday"].notna().all()

    # Test a case when the country is not on the predefined list
    with pytest.raises(ValueError):  # noqa
        get_holidays_dataframe(
            country="Spain",
            start_date="2024-12-1",
            end_date="2024-12-31",
        )

    # Test a case when the start_date is later than end_date
    with pytest.raises(ValueError):  # noqa
        get_holidays_dataframe(
            country="Norway",
            start_date="2024-12-31",
            end_date="2024-12-1",
        )


def test_get_calendar_dataframe_with_holiday_features() -> None:
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    country = "Sweden"

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
        "holiday",
        "days_until_next_holiday",
        "days_since_last_holiday",
        "is_squeeze_day",
        "is_long_weekend",
        "is_holiday_on_monday",
        "is_holiday_on_thursday",
        "is_holiday",
    ]

    df = get_calendar_dataframe_with_holiday_features(start_date, end_date, country, is_onehot_encode_holiday=False)

    assert isinstance(df, pd.DataFrame)
    assert set(df.columns.tolist()) == set(expected_columns)
    assert df["date"].dt.year.min() == pd.to_datetime(start_date).year
    assert df["date"].dt.year.max() == pd.to_datetime(end_date).year
    assert df[df["date"] == "2024-01-01"]["is_holiday"].values[0]

    # Test a case when the country is not on the predefined list
    with pytest.raises(ValueError):  # noqa
        get_calendar_dataframe_with_holiday_features(
            country="Spain",
            start_date="2024-12-1",
            end_date="2024-12-31",
        )

    # Test a case when the start_date is later than end_date
    with pytest.raises(ValueError):  # noqa
        get_calendar_dataframe_with_holiday_features(
            country="Norway",
            start_date="2024-12-31",
            end_date="2024-12-1",
        )


def test_get_weekly_calendar_with_holiday_features() -> None:
    start_date = "2024-01-01"
    end_date = "2025-06-10"
    country = "Norway"

    expected_columns = [
        "year",
        "week",
        "holiday_list",
        "month_number_monday",
        "month_number_sunday",
        "date_monday",
        "date_sunday",
        "num_holidays",
        "has_holiday_on_monday",
        "has_holiday_on_thursday",
        "has_squeeze_day",
        "has_long_weekend",
    ]

    df = get_weekly_calendar_with_holiday_features(start_date, end_date, country, include_onehot_encoded_holiday=False)

    assert isinstance(df, pd.DataFrame)
    assert set(df.columns.tolist()) == set(expected_columns)
