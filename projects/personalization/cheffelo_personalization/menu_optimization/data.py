from typing import Tuple

import numpy as np
import pandas as pd
from lmkgroup_ds_utils.azure.storage import BlobConnector
from lmkgroup_ds_utils.helpers.naming import camel2snake

from cheffelo_personalization.menu_optimization.utils.constants import (
    ING_COLUMN,
    PRICE_COLUMN,
    RATING_COLUMN,
    RECIPE_ID_COLUMN,
    TAX_COLUMN,
)
from cheffelo_personalization.menu_optimization.utils.paths import (
    get_recipe_bank_ingredients_url,
    get_recipe_bank_url,
)


def get_recipes_from_datalake(
    company_id: str, datalake_handler: BlobConnector
) -> pd.DataFrame:
    """Get recipe information from datalake

    Args:
        company_id (str): company id that we want to read pim data
        datalake_handler (BlobConnector, optional): Connection to datalake

    Returns:
        pd.DataFrame: dataframe of recipes information
    """
    recipe_bank_url = get_recipe_bank_url(company_id=company_id)
    df_recipes = datalake_handler.download_json_to_df(blob=str(recipe_bank_url))
    df_recipes = df_recipes.rename(
        columns={col: camel2snake(col) for col in df_recipes.columns}
    )
    return df_recipes


def get_ingredients_from_datalake(
    company_id: str, datalake_handler: BlobConnector
) -> pd.DataFrame:
    """Get ingredients information from datalake

    Args:
        company_id (str): company id that we want to read pim data
        datalake_handler (BlobConnector): connection to datalake

    Returns:
        pd.DataFrame: dataframe of ingredients information
    """
    recipe_bank_ingredients_url = get_recipe_bank_ingredients_url(company_id=company_id)
    df_ingredients = datalake_handler.download_json_to_df(
        blob=str(recipe_bank_ingredients_url)
    )
    df_ingredients = df_ingredients.rename(
        columns={col: camel2snake(col) for col in df_ingredients.columns}
    )
    return df_ingredients


def get_PIM_data(
    company_id: str, local: bool, datalake_handler: BlobConnector = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Reads PIM data from datalake, the recipe bank

    Args:
        company_id (str): company id that we want to read pim data
        local (bool): if we are running in local mode or through api
        datalake_handler (BlobConnector, optional): Connection to datalake if exist. Defaults to None.

    Returns:
        recipes, ingredients: returns two dataframe with recipes and ingredients information
    """
    if not datalake_handler:
        datalake_handler = BlobConnector(local=local)

    # Read recipe information
    df_recipes = get_recipes_from_datalake(
        company_id=company_id, datalake_handler=datalake_handler
    )

    # Read ingredients information
    df_ingredients = get_ingredients_from_datalake(
        company_id=company_id, datalake_handler=datalake_handler
    )

    return df_recipes, df_ingredients


def postprocess_recipe_data(
    df_recipes: pd.DataFrame, min_rating: int, use_nan_ratings: bool
) -> pd.DataFrame:
    """Postprocess recipe data, filter out recipes with low rating

    Args:
        df_recipes (pd.DataFrame): _description_
        min_rating (int): _description_
        use_nan_ratings (bool): _description_

    Returns:
        pd.DataFrame: dataframe of recipes
    """
    df_recipes[RATING_COLUMN] = df_recipes[RATING_COLUMN].fillna(
        min_rating if use_nan_ratings else np.nan
    )
    df_recipes = df_recipes[
        [RECIPE_ID_COLUMN, ING_COLUMN, TAX_COLUMN, RATING_COLUMN, PRICE_COLUMN]
    ].dropna()
    df_recipes[TAX_COLUMN] = df_recipes[TAX_COLUMN].astype(int)
    df_recipes[ING_COLUMN] = df_recipes[ING_COLUMN].astype(int)

    return df_recipes


def get_ingredient_distribution(ingredients, df, ingredients_not_found):
    ings = []
    not_full = []
    for ing in ingredients:
        actual = (
            df[f"{ING_COLUMN}_{ing[ING_COLUMN]}"].sum()
            if ing[ING_COLUMN] not in ingredients_not_found
            else 0
        )
        output = {
            ING_COLUMN: ing[ING_COLUMN],
            "wanted": ing["quantity"],
            "actual": int(actual),
        }
        ings.append(output)
        if output["wanted"] > output["actual"]:
            not_full.append(output[ING_COLUMN])

    return ings, not_full


def get_taxonomies_distribution(taxonomies, df, taxs_not_found):
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


def get_prices_distribution(prices, df, prices_not_found):
    prices_out = []
    not_full = []

    for price in prices:
        actual = (
            df[f"interval_({price['min']}, {price['max']})"].sum()
            if f"price_{price['min']}_{price['max']}" not in prices_not_found
            else 0
        )
        output = {
            "min": price["min"],
            "max": price["max"],
            "wanted": price["quantity"],
            "actual": int(actual),
        }
        prices_out.append(output)

        if output["wanted"] > output["actual"]:
            not_full.append((price["min"], price["max"]))

    print(prices_out, not_full)
    return prices_out, not_full
