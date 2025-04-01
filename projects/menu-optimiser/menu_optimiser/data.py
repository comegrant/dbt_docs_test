from typing import Tuple, Union, List, Dict

import numpy as np
import pandas as pd


from menu_optimiser.utils.constants import (
    ING_COLUMN,
    PRICE_CATEGORY_COLUMN,
    RATING_COLUMN,
    RECIPE_ID_COLUMN,
    TAX_COLUMN,
    UNIVERSE_COLUMN,
    COOKING_TIME_FROM_COLUMN,
    COOKING_TIME_TO_COLUMN,
    COOKING_TIME_COLUMN,
)


def postprocess_recipe_data(
    df_recipes: pd.DataFrame,
) -> pd.DataFrame:
    """Postprocess recipe data, filter out recipes with low rating

    Args:
        df_recipes (pd.DataFrame): DataFrame of recipes to postprocess

    Returns:
        pd.DataFrame: Postprocessed DataFrame of recipes
    """
    df_recipes.loc[:, RATING_COLUMN] = df_recipes[RATING_COLUMN].fillna(np.nan)
    df_recipes = df_recipes[  # type: ignore
        [
            RECIPE_ID_COLUMN,
            ING_COLUMN,
            TAX_COLUMN,
            RATING_COLUMN,
            PRICE_CATEGORY_COLUMN,
            UNIVERSE_COLUMN,
            COOKING_TIME_FROM_COLUMN,
            COOKING_TIME_TO_COLUMN,
        ]
    ]

    df_recipes = df_recipes.convert_dtypes()

    return df_recipes


def get_ingredient_distribution(
    ingredients: List[Dict[str, Union[int, float, str]]],
    df: pd.DataFrame,
    ingredients_not_found: List[int],
) -> Tuple[List[Dict[str, Union[int, float, str]]], List[int]]:
    """
    Calculate the actual and wanted quantities of each ingredient in a recipe.

    Args:
        ingredients (List[Dict[str, Union[int, float, str]]]): List of dictionaries where each dictionary contains the ingredient information
        df (pd.DataFrame): DataFrame of recipes
        ingredients_not_found (List[int]): List of ingredient ids that are not found in the recipes

    Returns:
        Tuple[List[Dict[str, Union[int, float, str]]], List[int]]: A tuple containing a list of dictionaries with the ingredient information
            and a list of ingredient ids that are not fulfilled
    """
    ings = []
    not_full = []
    for ing in ingredients:
        actual = (
            df[f"{ING_COLUMN}_{int(ing[ING_COLUMN])}"].sum()
            if int(ing[ING_COLUMN]) not in ingredients_not_found
            else 0
        )
        output = {
            ING_COLUMN: int(ing[ING_COLUMN]),
            "wanted": ing["quantity"],
            "actual": int(actual),
        }
        ings.append(output)
        if output["wanted"] > output["actual"]:
            not_full.append(output[ING_COLUMN])

    return ings, not_full


def get_taxonomies_distribution(
    taxonomies: List[Dict[str, Union[int, str]]],
    df: pd.DataFrame,
    taxs_not_found: List[int],
) -> Tuple[List[Dict[str, Union[int, str]]], List[int]]:
    """
    Get the difference between the number of wanted and actual recipes with the given taxonomies.

    Args:
        taxonomies (List[Dict[str, Union[int, str]]]): List of dictionaries where each dictionary contains the taxonomy information
        df (pd.DataFrame): DataFrame of recipes
        taxs_not_found (List[int]): List of taxonomy ids that are not found in the recipes

    Returns:
        Tuple[List[Dict[str, Union[int, str]]], List[int]]: A tuple containing a list of dictionaries with the taxonomy information
            and a list of taxonomy ids that are not fulfilled
    """
    taxs = []
    not_full = []
    for tax in taxonomies:
        actual = (
            df[f"{TAX_COLUMN}_{tax[TAX_COLUMN]}"].sum()
            if tax[TAX_COLUMN] not in taxs_not_found
            else 0
        )
        output = {
            TAX_COLUMN: tax[TAX_COLUMN],
            "taxonomy_type_id": tax["taxonomy_type_id"],
            "wanted": tax["quantity"],
            "actual": int(actual),
        }
        taxs.append(output)

        if output["wanted"] > output["actual"]:
            not_full.append(output[TAX_COLUMN])
    return taxs, not_full


def get_prices_distribution(
    prices: List[Dict[str, Union[int, str]]],
    df: pd.DataFrame,
    prices_not_found: List[int],
) -> Tuple[List[Dict[str, Union[int, str]]], List[int]]:
    """
    Get the difference between the number of wanted and actual recipes with the given prices.

    Args:
        prices: A list of dictionaries with the keys:
            - price_category_id: An integer representing the price category id.
            - quantity: An integer representing the number of recipes wanted.
        df: A pandas DataFrame containing the actual recipes.
        prices_not_found: A list of price category ids that are not found in the df.

    Returns:
        A tuple containing:
            - A list of dictionaries with the same keys as the prices argument, but with the actual quantities.
            - A list of price category ids that are not fulfilled.
    """
    prices_out = []
    not_full = []

    for price in prices:
        actual = (
            df[f"{PRICE_CATEGORY_COLUMN}_{int(price['price_category_id'])}"].sum()
            if price["price_category_id"] not in prices_not_found
            else 0
        )
        output = {
            "price_category_id": price["price_category_id"],
            "wanted": price["quantity"],
            "actual": int(actual),
        }
        prices_out.append(output)

        if output["wanted"] > output["actual"]:
            not_full.append(price["price_category_id"])

    return prices_out, not_full


def get_cooking_times_distribution(
    cooking_times: List[Dict[str, Union[int, str]]],
    df: pd.DataFrame,
    cooking_times_not_found: List[str],
) -> Tuple[List[Dict[str, Union[int, str]]], List[str]]:
    """
    Get the difference between the number of wanted and actual recipes with the given cooking_times.

    Args:
        cooking_times (List[Dict[str, Union[int, str]]]): A list of dictionaries, where each dictionary contains the keys
            'time_from', 'time_to', and 'quantity', representing the wanted recipes with those cooking times.
        df (pd.DataFrame): The dataframe to get the actual recipes from.
        cooking_times_not_found (List[str]): A list of cooking times that are not found in the dataframe.

    Returns:
        A tuple containing a list of dictionaries, where each dictionary contains the keys 'from', 'to', 'wanted', and
            'actual', representing the difference between the number of wanted and actual recipes, and a list of strings representing
            the cooking times that were not fulfilled.
    """
    cooking_times_out = []
    not_full = []

    for cooking_time in cooking_times:
        actual = (
            df[
                f"{COOKING_TIME_COLUMN}_{int(cooking_time['time_from'])}_{int(cooking_time['time_to'])}"
            ].sum()
            if f"{COOKING_TIME_COLUMN}_{int(cooking_time['time_from'])}_{int(cooking_time['time_to'])}"
            not in cooking_times_not_found
            else 0
        )
        output = {
            "from": cooking_time["time_from"],
            "to": cooking_time["time_to"],
            "wanted": cooking_time["quantity"],
            "actual": int(actual),
        }
        cooking_times_out.append(output)

        if output["wanted"] > output["actual"]:
            not_full.append(
                f"{COOKING_TIME_COLUMN}_{int(cooking_time['time_from'])}_{int(cooking_time['time_to'])}"
            )

    return cooking_times_out, not_full
