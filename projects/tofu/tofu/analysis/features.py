# pyright: reportAttributeAccessIssue=false
# pyright: reportCallIssue=false
# pyright: reportArgumentType=false
# pyright: reportPossiblyUnboundVariable=false

import pandas as pd
from time_machine.holidays import get_weekly_calendar_with_holiday_features


def add_holiday_features(df: pd.DataFrame, country: str) -> pd.DataFrame:
    min_year = df["menu_year"].min()
    max_year = df["menu_year"].max()
    start_date = f"{min_year}-01-01"
    end_date = f"{max_year}-12-31"
    df_weekly_holiday_calendar = get_weekly_calendar_with_holiday_features(
        start_date=start_date, end_date=end_date, country=country
    )
    df_weekly_holiday_calendar = df_weekly_holiday_calendar.rename(
        columns={
            "year": "menu_year",
            "week": "menu_week",
        }
    )

    df = df.merge(
        df_weekly_holiday_calendar,
        on=["menu_year", "menu_week"],
        how="inner",
    )
    df.loc[df["has_long_weekend"] > 0, "holiday_list"].apply(lambda x: x.append("Long Weekend"))
    df.loc[df["has_squeeze_day"] > 0, "holiday_list"].apply(lambda x: x.append("Squeeze Day"))

    df["num_holidays_effectively"] = df["num_holidays"]

    index_to_append_holiday_on_saturday = df[
        (df["has_holiday_on_saturday"] == 1)
        & (df["has_long_weekend"] == 0)
        & (df["christmas_day"] == 0)
        & (df["new_years_day"] == 0)
        & (df["second_day_of_christmas"] == 0)
    ].index
    df["is_holiday_overlap_with_weekend"] = 0
    df.loc[index_to_append_holiday_on_saturday, "holiday_list"].apply(lambda x: x.append("Holiday On Saturday"))
    df.loc[index_to_append_holiday_on_saturday, "is_holiday_overlap_with_weekend"] = 1
    df.loc[index_to_append_holiday_on_saturday, "num_holidays_effectively"] -= 1

    index_to_append_holiday_on_sunday = df[
        (df["has_holiday_on_sunday"] == 1)
        & (df["has_long_weekend"] == 0)
        & (df["christmas_day"] == 0)
        & (df["new_years_day"] == 0)
        & (df["second_day_of_christmas"] == 0)
    ].index
    df.loc[index_to_append_holiday_on_sunday, "holiday_list"].apply(lambda x: x.append("Holiday On Sunday"))
    df.loc[index_to_append_holiday_on_sunday, "is_holiday_overlap_with_weekend"] = 1
    df.loc[index_to_append_holiday_on_sunday, "num_holidays_effectively"] -= 1

    return df


def add_easter_features(
    df: pd.DataFrame,
    num_weeks_around_easter: int,
) -> pd.DataFrame:
    """Add features for the weeks around Easter.

    Args:
        df: DataFrame with menu_year and menu_week as well as holidays related to Easter
        num_weeks_around_easter: Number of weeks around Easter to consider.
    """
    # The week where Easter Sunday is is considered week 0 of easter.
    df.loc[(df["easter_sunday"] == 1), "num_weeks_since_easter"] = 0
    # Although there are multiple public holidays, they're always in the same week, count as 1 for now
    for year in df["menu_year"].unique():
        # Just to get Easter week. the dataframe might not always have Easter week
        # that's why using function to find instead of using the easter_sunday feature
        df_easter_week = get_weekly_calendar_with_holiday_features(
            start_date=f"{year}-01-01",
            end_date=f"{year}-12-31",
            country="Norway",  # Doesn't matter, all countries have the same Easter week
        )
        easter_week = df_easter_week[df_easter_week["easter_sunday"] == 1]["week"].values[0]
        df.loc[df["menu_year"] == year, "num_weeks_since_easter"] = (
            df.loc[df["menu_year"] == year, "menu_week"] - easter_week
        )
    df["is_around_easter"] = df["num_weeks_since_easter"].abs() <= num_weeks_around_easter
    return df
