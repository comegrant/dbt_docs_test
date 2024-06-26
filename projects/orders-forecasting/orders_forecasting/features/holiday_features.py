import logging
import re

import holidays
import numpy as np
import pandas as pd

from orders_forecasting.features.schemas import CountryHolidaysFeatures

logger = logging.getLogger(__name__)


def get_holiday_features(country: str, year_max: int, year_min: int) -> pd.DataFrame:
    df_holidays = get_country_holidays(
        country=country,
        year_max=year_max,
        year_min=year_min,
    )
    df_holidays = add_is_long_weekend(df_holidays=df_holidays)
    df_holidays[["year", "week"]] = pd.to_datetime(
        df_holidays["date"],
    ).dt.isocalendar()[["year", "week"]]
    df_holidays.drop(columns=["date"], inplace=True)
    df_holidays["holiday"] = df_holidays["holiday"].str.lower()
    df_holidays["holiday"] = df_holidays["holiday"].apply(
        lambda x: re.sub("[^0-9a-zA-Z]+", "_", x),
    )
    df_holidays = df_holidays[df_holidays["holiday"] != "sunday"]
    logger.info(df_holidays.columns)
    # Apparently Sunday is in the holiday calendar for Sweden ...
    # df_holidays["holiday"] = df_holidays["holiday"].str.replace(" ", "_")
    df_holidays_dummies = pd.get_dummies(df_holidays, columns=["holiday"], prefix="is")

    df_holidays_week = (
        df_holidays_dummies.groupby(["year", "week", "country"]).sum().reset_index()
    )

    df_holidays_week["num_holidays"] = df_holidays_week.drop(
        columns=["year", "week", "country", "is_long_weekend"],
    ).sum(axis=1)

    df_holidays_week.loc[:, "is_long_weekend"] = df_holidays_week[
        "is_long_weekend"
    ].apply(lambda x: min(1, x))

    df_holidays_week = df_holidays_week.astype({"week": "int", "year": "int"})

    df_holidays_week.sort_values(by=["year", "week"], inplace=True)

    # Convert dtypes to same as pandara schema
    df_holidays_week = df_holidays_week.astype(
        {
            col: str(dtype)
            for col, dtype in CountryHolidaysFeatures.to_schema().dtypes.items()
        }
    )

    return df_holidays_week


def get_country_holidays(
    country: str,
    year_min: int,
    year_max: int,
) -> pd.DataFrame:
    if country == "Norway":
        holiday_calendar = holidays.Norway(
            years=np.arange(year_min, year_max), language="en_US"
        )
    elif country == "Sweden":
        holiday_calendar = holidays.Sweden(
            years=np.arange(year_min, year_max), language="en_US"
        )
    elif country == "Denmark":
        holiday_calendar = holidays.Denmark(
            years=np.arange(year_min, year_max), language="en_US"
        )
    dates = []
    names = []
    for a_date, name in holiday_calendar.items():
        dates.append(a_date)
        names.append(name)
    df_holidays = pd.DataFrame(
        data={
            "country": [country] * len(dates),
            "date": dates,
            "holiday": names,
        },
    )
    return df_holidays


def add_is_long_weekend(df_holidays: pd.DataFrame) -> pd.DataFrame:
    dow_friday = 4
    dow_monday = 0

    df_holidays["dow"] = pd.to_datetime(df_holidays["date"]).dt.weekday
    df_holidays["is_long_weekend"] = 0
    # mondays or Friday
    is_mon = df_holidays["dow"] == dow_monday
    is_fri = df_holidays["dow"] == dow_friday
    df_holidays.loc[is_mon, "is_long_weekend"] = 1
    df_holidays.loc[is_fri, "is_long_weekend"] = 1

    # Easter's always a long weekend
    is_easter = df_holidays["holiday"].str.lower().str.contains("easter")
    df_holidays.loc[is_easter, "is_long_weekend"] = 1
    df_holidays = df_holidays.drop(columns="dow")

    return df_holidays
