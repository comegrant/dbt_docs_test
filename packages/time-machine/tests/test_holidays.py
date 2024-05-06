import holidays
import pandas as pd
import pytest
from time_machine.holidays import (
    get_calendar_dataframe_with_holiday_features,
    get_holidays_dataframe,
    get_holidays_dictionary,
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
        "days_until_next_holiday",
        "days_since_last_holiday",
        "is_squeeze_day",
        "is_long_weekend",
        "nyårsdagen",
        "trettondedag_jul",
    ]

    df = get_calendar_dataframe_with_holiday_features(start_date, end_date, country)

    assert isinstance(df, pd.DataFrame)
    assert set(df.columns.tolist()) == set(expected_columns)
    assert df["date"].dt.year.min() == pd.to_datetime(start_date).year
    assert df["date"].dt.year.max() == pd.to_datetime(end_date).year

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
