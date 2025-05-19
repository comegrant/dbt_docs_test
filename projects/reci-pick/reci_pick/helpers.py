from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
import pytz
from dateutil.relativedelta import MO, relativedelta


def get_dict_values_as_array(look_up_dict: dict, key_list: list[Any]) -> np.array:
    """Retrieve embeddings for a list of IDs in a vectorized manner."""
    ids_array = np.array(key_list)  # Flatten the array to handle 2-D input
    value_list = np.array([look_up_dict.get(a_key) for a_key in ids_array])
    return value_list  # Reshape back to the original shape


def get_dict_values_as_list(
    look_up_dict: dict,
    key_list: list[Any],
    is_flatten_list: bool = False,
    unique_values_only: bool = False,
) -> list[Any]:
    """Retrieve embeddings for a list of IDs in a vectorized manner."""
    if is_flatten_list:
        value_list = []
        for a_key in key_list:
            value_list.extend(look_up_dict.get(a_key))
        if unique_values_only:
            value_list = list(set(value_list))

    else:
        value_list = [look_up_dict.get(a_key) for a_key in key_list]
    return value_list  # Reshape back to the original shape


def strip_prefix(column_name_list: list[str], prefix: str) -> list[str]:
    new_names = []
    for column_name in column_name_list:
        if column_name.startswith(prefix):
            start_index = len(prefix)
            new_names.append(column_name[start_index:])
        else:
            new_names.append(column_name)
    return new_names


def combine_dictionaries(dict1: dict, dict2: dict) -> dict:
    combined_dict = {}
    for key in set(dict1) & set(dict2):  # Intersection of keys from both dictionaries
        combined_dict[key] = np.concatenate([dict1.get(key), dict2.get(key)])  # Concatenate values
    return combined_dict


def get_date_from_year_week(
    year: int,
    week_number: int,
    weekday: int | None = 1,
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


def has_two_columns_intersection(
    row: pd.Series,
    col1: str,
    col2: str,
) -> pd.Series:
    col1_cleaned = row[col1] if row[col1] is not None else []
    col2_cleaned = row[col2] if row[col2] is not None else []
    intersects = set(col1_cleaned) & set(col2_cleaned)
    has_intersect = len(intersects) > 0
    return has_intersect
