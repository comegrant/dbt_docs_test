# pyright: ignore
# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false

import datetime
from typing import Optional, Union

import holidays
import pandas as pd
import pytz

from time_machine.calendars import get_calendar_dataframe


def get_holidays_dictionary(start_date: str, end_date: str, country: Optional[Union[str, list[str]]] = None) -> dict:
    """
    Returns a dictionary containing holiday objects for the specified countries between the specified date range.

    Parameters:
    - start_date (str): The start date of the date range in the format 'YYYY-MM-DD'.
    - end_date (str): The end date of the date range in the format 'YYYY-MM-DD'.
    - country (str or list of str): The country or countries for which to retrieve the holiday objects.
        Accepts only "Sweden", "Norway", and "Denmark".

    Raises:
    - ValueError: If the end date is before the start date.
    - ValueError: If the country is not "Sweden", "Norway", and "Denmark"

    Returns:
    - dict: A dictionary containing holiday objects for the specified countries.
    """
    if country is None:
        country = ["Sweden", "Norway", "Denmark"]
    if pd.to_datetime(end_date) < pd.to_datetime(start_date):
        raise ValueError("End date cannot be before start date.")

    date_range = pd.date_range(start=start_date, end=end_date)
    country_holiday_objects = {
        "Sweden": holidays.Sweden(years=date_range.year.tolist(), include_sundays=False, language="en_US"),
        "Norway": holidays.Norway(years=date_range.year.tolist(), include_sundays=False, language="en_US"),
        "Denmark": holidays.Denmark(years=date_range.year.tolist(), language="en_US"),
    }

    if country not in country_holiday_objects:
        raise ValueError(
            "Invalid country. Accepts only 'Sweden', 'Norway', and 'Denmark'.",
        )

    holiday_obj = country_holiday_objects[country]

    return holiday_obj


def get_holiday_object_list(
    start_date: str,
    end_date: str,
    country: str,
) -> list[tuple[pd.Timestamp, int, int, Optional[str]]]:
    holiday_obj = get_holidays_dictionary(
        start_date=start_date,
        end_date=end_date,
        country=country,
    )
    date_range = pd.date_range(start=start_date, end=end_date)
    holiday_object_list = [
        (date, date.isocalendar().year, date.isocalendar().week, holiday_obj.get(date))
        for date in date_range
        if date in holiday_obj
    ]

    return holiday_object_list


def get_holidays_dataframe(
    start_date: str,
    end_date: str,
    country: str,
) -> pd.DataFrame:
    """
    Returns a DataFrame with holidays for a given date range.

    Args:
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
        country (str, optional): The country or countries for which to retrieve holidays.
            Accepts only "Sweden", "Norway", and "Denmark".

    Raises:
    - ValueError: If the end date is before the start date.
    - ValueError: If the country is not "Sweden", "Norway", and "Denmark"

    Returns:
        pd.DataFrame: A DataFrame with the following columns: "date", "year", "week", "holiday".
    """

    holiday_object_list = get_holiday_object_list(start_date=start_date, end_date=end_date, country=country)

    df_holidays = pd.DataFrame(
        holiday_object_list,
        columns=["date", "year", "week", "holiday"],
    )

    return df_holidays


def get_holiday_date_list(holiday_obj: dict) -> list[pd.Timestamp]:
    holiday_list = [pd.to_datetime(item) for item in list(holiday_obj.keys())]
    return holiday_list


def get_calendar_dataframe_with_holiday_features(
    start_date: str,
    end_date: str,
    country: str,
    is_onehot_encode_holiday: bool | None = True,
) -> pd.DataFrame:
    """
    Returns a DataFrame with calendar and holiday features for a given date range.
        Each row is a date and the holidays are one hot encoded.

    Args:
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
        country (str, optional): The country or countries for which to retrieve holidays.
            Accepts only "Sweden", "Norway", and "Denmark".
        is_onehot_encode_holiday (bool, optional): Whether to one-hot encode the holiday column.
            Default is True. If true, the column "holiday" will be dropped.
    Raises:
    - ValueError: If the end date is before the start date.
    - ValueError: If the country is not "Sweden", "Norway", and "Denmark"

    Returns:
        pd.DataFrame: A DataFrame with the following columns: 'date', 'year', 'week',
        'datekey', 'day_of_week', 'weekday_name', 'quarter', 'month_number', 'month_name'
        "days_until_next_holiday", "days_since_last_holiday", "is_squeeze_day", "is_long_weekend","is_holiday"
        and one-hot encoded columns for each holiday if is_onehot_encode_holiday is True; or 'holiday' column
        if is_onehot_encode_holiday is False.
    """

    holiday_obj = get_holidays_dictionary(
        start_date=start_date,
        end_date=end_date,
        country=country,
    )

    df_holidays = get_calendar_dataframe(start_date=start_date, end_date=end_date)
    df_holidays = df_holidays[
        [
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
    ]

    df_holidays = add_holiday_feature(df_holidays=df_holidays, holiday_obj=holiday_obj)
    holiday_list = get_holiday_date_list(holiday_obj=holiday_obj)
    df_holidays = add_days_until_next_holiday_feature(df_holidays=df_holidays, holiday_list=holiday_list)
    df_holidays = add_days_since_last_holiday_feature(df_holidays=df_holidays, holiday_list=holiday_list)
    df_holidays = add_is_squeeze_day_feature(df_holidays=df_holidays, holiday_list=holiday_list)
    df_holidays = add_is_long_weekend_feature(df_holidays=df_holidays)

    df_holidays = add_is_holiday_on_monday_feature(df_holidays=df_holidays)
    df_holidays = add_is_holiday_on_thursday_feature(df_holidays=df_holidays)

    df_holidays = add_is_holiday_on_saturday_feature(df_holidays=df_holidays)
    df_holidays = add_is_holiday_on_sunday_feature(df_holidays=df_holidays)
    df_holidays = add_is_holiday_on_weekend(df_holidays=df_holidays)
    df_holidays["is_holiday"] = df_holidays["holiday"].notna()
    if is_onehot_encode_holiday:
        one_hot = pd.get_dummies(df_holidays["holiday"])
        one_hot.columns = one_hot.columns.str.replace("'", "").str.lower()
        one_hot.columns = one_hot.columns.str.replace(" ", "_").str.lower()
        df_holidays = pd.concat([df_holidays, one_hot], axis=1)
        df_holidays = df_holidays.drop(columns=["holiday"])

    return df_holidays


def get_weekly_calendar_with_holiday_features(
    start_date: str,
    end_date: str,
    country: str,
    include_onehot_encoded_holiday: bool | None = True,
) -> pd.DataFrame:
    """
    Returns a DataFrame with calendar and holiday features for a given date range.
    Each row is a week and the holidays are one hot encoded.

    Args:
        df_holiday_features (pd.DataFrame): a pandas dataframe with calendar and holiday features
        start_date (str): the start date of the date range
        end_date (str): the end date of the date range
    """
    df_holiday_features = get_calendar_dataframe_with_holiday_features(
        start_date=start_date, end_date=end_date, country=country, is_onehot_encode_holiday=False
    )
    df_groupped_holiday_list = (
        df_holiday_features.groupby(["year", "week"])
        .agg({"holiday": lambda x: x.dropna().tolist()})
        .reset_index()
        .rename(columns={"holiday": "holiday_list"})
    )
    df_groupped_date_month = (
        df_holiday_features.groupby(["year", "week"])
        .agg(
            {
                "month_number": ["min", "max"],
                "date": ["min", "max"],
            }
        )
        .reset_index()
    )
    df_groupped_date_month.columns = [
        "year",
        "week",
        "month_number_monday",
        "month_number_sunday",
        "date_monday",
        "date_sunday",
    ]
    df_holiday_binary_features_summed = (
        df_holiday_features.groupby(["year", "week"])
        .agg(
            {
                "is_holiday": "sum",
                "is_holiday_on_monday": "sum",
                "is_holiday_on_thursday": "sum",
                "is_holiday_on_saturday": "sum",
                "is_holiday_on_sunday": "sum",
                "is_holiday_on_weekend": "sum",
                "is_squeeze_day": "sum",
                "is_long_weekend": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                "is_holiday": "num_holidays",
                "is_holiday_on_monday": "has_holiday_on_monday",
                "is_holiday_on_thursday": "has_holiday_on_thursday",
                "is_holiday_on_saturday": "has_holiday_on_saturday",
                "is_holiday_on_sunday": "has_holiday_on_sunday",
                "is_holiday_on_weekend": "has_holiday_on_weekend",
                "is_squeeze_day": "has_squeeze_day",
                "is_long_weekend": "has_long_weekend",
            }
        )
    )
    df_weekly_calendar_with_holiday_features = df_groupped_holiday_list.merge(
        df_groupped_date_month, on=["year", "week"], how="left"
    ).merge(df_holiday_binary_features_summed, on=["year", "week"], how="left")
    if include_onehot_encoded_holiday:
        # We want to get the one-hot encoded holidays for at least the entire year to ensure that
        # the one-hot encoded holidays produce consistent columns.
        timezone = pytz.timezone("UTC")
        holiday_start_date = datetime.datetime(pd.to_datetime(start_date).year, 1, 1, tzinfo=timezone)
        holiday_end_date = datetime.datetime(pd.to_datetime(end_date).year, 12, 31, tzinfo=timezone)

        df_holiday_features_longer_period = get_calendar_dataframe_with_holiday_features(
            start_date=holiday_start_date.strftime("%Y-%m-%d"),
            end_date=holiday_end_date.strftime("%Y-%m-%d"),
            country=country,
            is_onehot_encode_holiday=False,
        )
        df_dummies = pd.get_dummies(df_holiday_features_longer_period["holiday"])
        df_dummies.columns = df_dummies.columns.str.replace("'", "").str.lower()
        df_dummies.columns = df_dummies.columns.str.replace(" ", "_").str.lower()
        onehot_columns = df_dummies.columns

        df_holidays_onehot_by_week_longer_period = (
            pd.concat([df_holiday_features_longer_period, df_dummies], axis=1)
            .groupby(["year", "week"])[onehot_columns]
            .sum()
            .reset_index()
        )
        df_weekly_calendar_with_holiday_features = df_weekly_calendar_with_holiday_features.merge(
            df_holidays_onehot_by_week_longer_period, how="left", on=["year", "week"]
        )

    return df_weekly_calendar_with_holiday_features


def add_holiday_feature(df_holidays: pd.DataFrame, holiday_obj: dict) -> pd.DataFrame:
    """Adds a holiday column to the dataframe.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column
        holiday_obj (dict): a dictionary of holiday objects for the specified countries

    Returns:
        pd.DataFrame: a pandas dataframe with a "holiday" column added to it which contains:
        - the holiday name for the date if the date is a holiday
        - None if the date is not a holiday
    """
    df_holidays["holiday"] = [holiday_obj.get(date) for date in df_holidays["date"]]
    return df_holidays


def add_days_until_next_holiday_feature(df_holidays: pd.DataFrame, holiday_list: list[pd.Timestamp]) -> pd.DataFrame:
    """Adds a days_until_next_holiday column to the dataframe.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column
        holiday_list (list[pd.Timestamp]): a list of holiday dates

    Returns:
        pd.DataFrame: a pandas dataframe with a "days_until_next_holiday" column added to it which contains:
        - the number of days until the next holiday
        - None if the date is not a holiday
    """
    df_holidays["days_until_next_holiday"] = [
        abs(
            (
                date
                - min(
                    [d for d in holiday_list if d > date],
                    default=max(df_holidays["date"]),
                )
            ).days,
        )
        for date in df_holidays["date"]
    ]
    return df_holidays


def add_days_since_last_holiday_feature(df_holidays: pd.DataFrame, holiday_list: list[pd.Timestamp]) -> pd.DataFrame:
    """Adds a days_since_last_holiday column to the dataframe.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column
        holiday_list (list[pd.Timestamp]): a list of holiday dates

    Returns:
        pd.DataFrame: a pandas dataframe with a "days_since_last_holiday" column added to it which contains:
        - the number of days since the last holiday
        - None if the date is not a holiday
    """
    df_holidays["days_since_last_holiday"] = [
        abs(
            (
                date
                - max(
                    [d for d in holiday_list if d < date],
                    default=min(df_holidays["date"]),
                )
            ).days,
        )
        for date in df_holidays["date"]
    ]
    return df_holidays


def add_is_squeeze_day_feature(df_holidays: pd.DataFrame, holiday_list: list[pd.Timestamp]) -> pd.DataFrame:
    """Adds a is_squeeze_day column to the dataframe.
    A squeeze day is a day that is a weekday and is "squeezed in between" a holiday and a weekend.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column
        holiday_list (list[pd.Timestamp]): a list of holiday dates

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_squeeze_day" column added to it which contains:
        - True if the date is a squeeze day
        - False if the date is not a squeeze day
    """

    if "holiday" not in df_holidays.columns:
        raise ValueError("holiday column not found in df_holidays")

    prev_holidays = [d + pd.Timedelta(days=1) for d in holiday_list if d.weekday() not in [5, 6]]
    prev_holidays += [d - pd.Timedelta(days=1) for d in holiday_list if d.weekday() not in [5, 6]]
    df_holidays["is_squeeze_day"] = (
        (df_holidays["date"].apply(lambda x: x.dayofweek).isin([0, 4]))
        & (df_holidays["holiday"].isna())
        & (df_holidays["date"].isin(prev_holidays))
    )
    return df_holidays


def add_is_long_weekend_feature(
    df_holidays: pd.DataFrame,
) -> pd.DataFrame:
    """Adds a is_long_weekend column to the dataframe.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_long_weekend" column added to it which contains:
        - True if the date is a long weekend
        - False if the date is not a long weekend
    """

    if "holiday" not in df_holidays.columns:
        raise ValueError("holiday column not found in df_holidays")

    df_holidays["is_long_weekend"] = (df_holidays["date"].apply(lambda x: x.dayofweek).isin([0, 4])) & (
        df_holidays["holiday"].notna()
    )
    return df_holidays


def add_is_holiday_on_monday_feature(df_holidays: pd.DataFrame) -> pd.DataFrame:
    """Adds a is_holiday_on_monday column to the dataframe.
    The reason we have this feature is because when a holiday is on a Monday, it is both a long weekend
    and people might not be home for their delivery.
    This feature can be very useful when it comes to demand forecasting.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_holiday_on_monday" column added to it which contains:
        - True if the date is a holiday on a Monday
        - False if the date is not a holiday on a Monday
    """
    df_holidays["is_holiday_on_monday"] = (df_holidays["date"].apply(lambda x: x.dayofweek) == 0) & (
        df_holidays["holiday"].notna()
    )
    return df_holidays


def add_is_holiday_on_thursday_feature(df_holidays: pd.DataFrame) -> pd.DataFrame:
    """Adds a is_holiday_on_thursday column to the dataframe.
    The reason we have this feature is because when a holiday is on a Thursday, it is both a long weekend
    and people might not be home for their delivery.
    This feature can be very useful when it comes to demand forecasting.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_holiday_on_thursday" column added to it which contains:
        - True if the date is a holiday on a Monday
        - False if the date is not a holiday on a Monday
    """
    df_holidays["is_holiday_on_thursday"] = (df_holidays["date"].apply(lambda x: x.dayofweek) == 3) & (  # noqa
        df_holidays["holiday"].notna()
    )
    return df_holidays


def add_is_holiday_on_weekend(df_holidays: pd.DataFrame) -> pd.DataFrame:
    """Adds a is_holiday_on_weekend column to the dataframe.
    The reason we have this feature is because when a holiday is on a Thursday, it is both a long weekend
    and people might not be home for their delivery.
    This feature can be very useful when it comes to demand forecasting.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_holiday_on_thursday" column added to it which contains:
        - True if the date is a holiday on a Monday
        - False if the date is not a holiday on a Monday
    """
    df_holidays["is_holiday_on_weekend"] = (df_holidays["date"].apply(lambda x: x.dayofweek) >= 5) & (  # noqa
        df_holidays["holiday"].notna()
    )
    return df_holidays


def add_is_holiday_on_saturday_feature(df_holidays: pd.DataFrame) -> pd.DataFrame:
    """Adds a is_holiday_on_saturday column to the dataframe.
    The reason we have this feature is because when a holiday is on a Thursday, it is both a long weekend
    and people might not be home for their delivery.
    This feature can be very useful when it comes to demand forecasting.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_holiday_on_thursday" column added to it which contains:
        - True if the date is a holiday on a Monday
        - False if the date is not a holiday on a Monday
    """
    df_holidays["is_holiday_on_saturday"] = (df_holidays["date"].apply(lambda x: x.dayofweek) == 5) & (  # noqa
        df_holidays["holiday"].notna()
    )
    return df_holidays


def add_is_holiday_on_sunday_feature(df_holidays: pd.DataFrame) -> pd.DataFrame:
    """Adds a is_holiday_on_sunday column to the dataframe.
    The reason we have this feature is because when a holiday is on a Thursday, it is both a long weekend
    and people might not be home for their delivery.
    This feature can be very useful when it comes to demand forecasting.

    Args:
        df_holidays (pd.DataFrame): a pandas dataframe with a "date" column

    Returns:
        pd.DataFrame: a pandas dataframe with a "is_holiday_on_thursday" column added to it which contains:
        - True if the date is a holiday on a Monday
        - False if the date is not a holiday on a Monday
    """
    df_holidays["is_holiday_on_sunday"] = (df_holidays["date"].apply(lambda x: x.dayofweek) == 6) & (  # noqa
        df_holidays["holiday"].notna()
    )
    return df_holidays
