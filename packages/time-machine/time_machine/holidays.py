import holidays
import pandas as pd


def get_holidays_dictionary(
    start_date: str,
    end_date: str,
    country: str = ["Sweden", "Norway", "Denmark"],
) -> dict:
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
    if pd.to_datetime(end_date) < pd.to_datetime(start_date):
        raise ValueError("End date cannot be before start date.")

    date_range = pd.date_range(start=start_date, end=end_date)
    country_holiday_objects = {
        "Sweden": holidays.Sweden(
            years=date_range.year.tolist(),
            include_sundays=False,
        ),
        "Norway": holidays.Norway(
            years=date_range.year.tolist(),
            include_sundays=False,
        ),
        "Denmark": holidays.Denmark(years=date_range.year.tolist()),
    }

    if country not in country_holiday_objects:
        raise ValueError(
            "Invalid country. Accepts only 'Sweden', 'Norway', and 'Denmark'.",
        )

    holiday_obj = country_holiday_objects[country]

    return holiday_obj


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
    date_range = pd.date_range(start=start_date, end=end_date)
    holiday_obj = get_holidays_dictionary(
        start_date=start_date,
        end_date=end_date,
        country=country,
    )

    holiday_list = [
        (date, date.isocalendar().year, date.isocalendar().week, holiday_obj.get(date))
        for date in date_range
        if date in holiday_obj
    ]
    df_holidays = pd.DataFrame(
        holiday_list,
        columns=["date", "year", "week", "holiday"],
    )

    return df_holidays


def get_calendar_dataframe_with_holiday_features(
    start_date: str,
    end_date: str,
    country: str,
) -> pd.DataFrame:
    """
    Returns a DataFrame with calendar and holiday features for a given date range.
        Each row is a date and the holidays are one hot encoded.

    Args:
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
        country (str, optional): The country or countries for which to retrieve holidays.
            Accepts only "Sweden", "Norway", and "Denmark".

    Raises:
    - ValueError: If the end date is before the start date.
    - ValueError: If the country is not "Sweden", "Norway", and "Denmark"

    Returns:
        pd.DataFrame: A DataFrame with the following columns: "date", "year", "week",
        "days_until_next_holiday", "days_since_last_holiday", "is_squeeze_day", "is_long_weekend",
        and one-hot encoded columns for each holiday.
    """

    date_range = pd.date_range(start=start_date, end=end_date)
    holiday_obj = get_holidays_dictionary(
        start_date=start_date,
        end_date=end_date,
        country=country,
    )

    holiday_list = [date for date in date_range if date in holiday_obj]

    df_holidays = pd.DataFrame(
        [
            (
                date,
                date.isocalendar().year,
                date.isocalendar().week,
                holiday_obj.get(date),
                abs(
                    (
                        date
                        - min(
                            [d for d in holiday_list if d > date],
                            default=max(date_range),
                        )
                    ).days,
                ),
                (
                    date
                    - max(
                        [d for d in holiday_list if d < date],
                        default=min(date_range),
                    )
                ).days,
            )
            for date in date_range
        ],
        columns=[
            "date",
            "year",
            "week",
            "holiday",
            "days_until_next_holiday",
            "days_since_last_holiday",
        ],
    )

    prev_holidays = [
        d + pd.Timedelta(days=1) for d in holiday_list if d.weekday() not in [5, 6]
    ]
    prev_holidays += [
        d - pd.Timedelta(days=1) for d in holiday_list if d.weekday() not in [5, 6]
    ]
    df_holidays["is_squeeze_day"] = (
        (df_holidays["date"].apply(lambda x: x.dayofweek).isin([0, 4]))
        & (df_holidays["holiday"].isna())
        & (df_holidays["date"].isin(prev_holidays))
    )

    df_holidays["is_long_weekend"] = (
        df_holidays["date"].apply(lambda x: x.dayofweek).isin([0, 4])
    ) & (df_holidays["holiday"].notna())

    one_hot = pd.get_dummies(df_holidays["holiday"])
    one_hot.columns = one_hot.columns.str.replace(" ", "_").str.lower()
    df_holidays = pd.concat([df_holidays, one_hot], axis=1)
    df_holidays = df_holidays.drop(columns=["holiday"])

    return df_holidays
