import logging
from typing import List

import pandas as pd
from lmkgroup_ds_utils.azure.storage import BlobConnector

from cheffelo_personalization.menu_optimization.data import (
    get_ingredient_distribution,
    get_PIM_data,
    get_taxonomies_distribution,
    postprocess_recipe_data,
    get_prices_distribution,
)
from cheffelo_personalization.menu_optimization.utils.constants import (
    ING_COLUMN,
    MAX_RATING,
    PRICE_COLUMN,
    RATING_COLUMN,
    RECIPE_ID_COLUMN,
    TAX_COLUMN,
)

logger = logging.getLogger(__name__)


def exclude_recipes(
    df: pd.DataFrame,
    df_ing: pd.DataFrame,
    min_rating: int,
    min_recipe_cost: float,
    max_recipe_cost: float,
    ingredients: List,
) -> pd.DataFrame:
    """Exclude recipes

    Args:
        df (_type_): _description_
        df_ing (_type_): _description_
        min_rating (_type_): _description_
        min_recipe_cost (_type_): _description_
        max_recipe_cost (_type_): _description_
        ingredients (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_ing = df_ing[df_ing["ingredient_id"].isin(ingredients)]

    df_after_excluded = df[
        ~df[RECIPE_ID_COLUMN].isin(df_ing[RECIPE_ID_COLUMN].unique())
    ]
    df_after_excluded = df_after_excluded[
        df_after_excluded[RATING_COLUMN] >= min_rating
    ]

    df_after_excluded = df_after_excluded[
        (df_after_excluded[PRICE_COLUMN] >= min_recipe_cost)
        & (df_after_excluded[PRICE_COLUMN] < max_recipe_cost)
    ]

    return df_after_excluded


def assign_interval(value, intervals):
    for i, interval in enumerate(intervals):
        if interval[0] <= value <= interval[1]:
            return intervals[i]
    return "Other"


def get_dummies(df, df_grouped, dist):
    dummies = pd.get_dummies(data=df, columns=[TAX_COLUMN, ING_COLUMN, "interval"])

    dummies_df = (
        df[[RECIPE_ID_COLUMN]]
        .set_index(RECIPE_ID_COLUMN)
        .join(
            dummies.drop([RATING_COLUMN, PRICE_COLUMN], axis=1).set_index(
                RECIPE_ID_COLUMN
            )
        )
    )

    out = dummies_df.groupby([RECIPE_ID_COLUMN]).sum()
    out = out.applymap(lambda x: 1 if x > 0 else x)
    out = out.join(df_grouped)
    out["n_overlay"] = out[list(dist.keys())].sum(axis=1) - 1
    return out


def update_dist(dist, df, wanted_df):
    actual = {}
    wanted_df = wanted_df.drop(df.index)
    for protein in dist.keys():
        new_quantity = dist[protein] - df[protein].sum()
        if not new_quantity:
            wanted_df = wanted_df[wanted_df[protein] == 0]
            continue
        actual[protein] = new_quantity
    return actual, wanted_df


def get_message(
    ings_not_full,
    taxs_not_full,
    prices_not_full,
    ings_not_found,
    taxs_not_found,
    prices_not_found,
    recipes_not_full,
):
    if (
        not len(
            ings_not_full
            + taxs_not_full
            + ings_not_found
            + taxs_not_found
            + prices_not_found
        )
        and not recipes_not_full
    ):
        return (0, "SUCCESS")

    msg = ""

    if len(taxs_not_found):
        msg += f"TAXONOMIES {str(taxs_not_found)[1:-1]} not found on recipe bank."
    if len(ings_not_found):
        msg += f"INGREDIENTS {str(ings_not_found)[1:-1]} not found on recipe bank."
    if len(prices_not_found):
        msg += (
            f"RECIPES WITH PRICES BETWEEN {prices_not_found} not found on recipe bank."
        )
    if recipes_not_full:
        msg += "NOT ENOUGH RECIPES."
    if len(taxs_not_full):
        msg += f"TAXONOMIES {str(taxs_not_full)[1:-1]} not fulfilled."
    if len(ings_not_full):
        msg += f"INGREDIENTS {str(ings_not_full)[1:-1]} not fulfilled."
    if len(prices_not_full):
        msg += f"RECIPES WITH PRICES BETWEEN {prices_not_full} not fullfilled."

    return (1, f"WARNING! {msg}")


def get_output(
    company_id,
    week,
    year,
    status,
    msg,
    ings_out=[],
    tax_out=[],
    prices_out=[],
    recipes=[],
):
    return {
        "company_id": company_id,
        "week": week,
        "year": year,
        "ingredients": ings_out,
        "taxonomies": tax_out,
        "prices": prices_out,
        "STATUS": status,
        "STATUS_MSG": msg,
        "recipes": recipes,
    }


def generate_menu(
    num_recipes: int,
    rules: dict,
    week: int,
    year: int,
    company_id: str,
    local: bool = False,
    datalake_handler: BlobConnector = None,
):
    """
    Generate a menu combination from all recipes.
    """

    # Parse input data
    ingredients = rules["main_ingredients"]
    taxonomies = rules["taxonomies"]
    ingredients_to_exclude = [
        d["ingredient_id"] for d in rules["ingredients"] if d["status_id"] == 3
    ]
    min_rating = rules["min_average_rating"]
    min_recipe_cost = 0
    max_recipe_cost = 1000  # TAKE OFF WHEN FINAL VERSION IS DONE

    prices = rules["prices"]
    use_nan_rating = False
    if "include_recipes_without_rating" in rules.keys():
        use_nan_rating = rules["include_recipes_without_rating"]

    dist = {}

    if min_rating > MAX_RATING:
        return get_output(company_id, week, year, 3, "AVERAGE RATING SCALE IS 0-5")

    df_recipes, df_ing = get_PIM_data(
        company_id=company_id, local=local, datalake_handler=datalake_handler
    )

    logger.info(rules.keys())
    df_recipes = postprocess_recipe_data(
        df_recipes=df_recipes,
        min_rating=rules["min_average_rating"],
        use_nan_ratings=use_nan_rating,
    )

    # PIM_data = PIM_data[~PIM_data[ING_COLUMN].isin(ingredients_to_exclude)]
    # df_after_excluded = exclude_recipes(PIM_data, min_rating, ingredients_to_exclude, min_recipe_cost, max_recipe_cost)

    PIM_data_excluded = exclude_recipes(
        df_recipes,
        df_ing,
        min_rating,
        min_recipe_cost,
        max_recipe_cost,
        ingredients_to_exclude,
    )
    # Prices and then ingredients first to prioritize
    ings_not_found = []
    taxs_not_found = []
    prices_not_found = []
    prices_interval = []
    for price in prices:
        if len(
            PIM_data_excluded[
                (PIM_data_excluded[PRICE_COLUMN] >= price["min"])
                & (PIM_data_excluded[PRICE_COLUMN] < price["max"])
            ]
        ):
            dist[f"interval_({price['min']}, {price['max']})"] = price["quantity"]
            prices_interval.append((price["min"], price["max"]))
        else:
            prices_not_found.append(f"price_{price['min']}_{price['max']}")

    prices_interval = sorted(prices_interval, key=lambda x: x[0])

    for ingredient in ingredients:
        if ingredient[ING_COLUMN] in PIM_data_excluded[ING_COLUMN].unique():
            dist[f"{ING_COLUMN}_{ingredient[ING_COLUMN]}"] = ingredient["quantity"]
        else:
            ings_not_found.append(ingredient[ING_COLUMN])
    for taxonomy in taxonomies:
        if taxonomy[TAX_COLUMN] in PIM_data_excluded[TAX_COLUMN].unique():
            dist[f"{TAX_COLUMN}_{taxonomy[TAX_COLUMN]}"] = taxonomy["quantity"]
        else:
            taxs_not_found.append(taxonomy[TAX_COLUMN])

    df_recipe_group = (
        PIM_data_excluded[[RECIPE_ID_COLUMN, RATING_COLUMN, PRICE_COLUMN]]
        .groupby(RECIPE_ID_COLUMN)
        .mean()
    )

    PIM_data_excluded["interval"] = PIM_data_excluded["price"].apply(
        assign_interval, intervals=prices_interval
    )
    out = get_dummies(PIM_data_excluded, df_recipe_group, dist)

    wanted_df = out[out["n_overlay"] >= 0]

    final_df = pd.DataFrame()
    while len(final_df) < num_recipes and len(wanted_df) > 0:
        mult_df = wanted_df[wanted_df["n_overlay"] == max(wanted_df["n_overlay"])]
        row = mult_df.sample(1)
        final_df = pd.concat([final_df, row])

        dist, wanted_df = update_dist(dist, row, wanted_df)

        wanted_df["n_overlay"] = wanted_df[list(dist.keys())].sum(axis=1) - 1
        wanted_df = wanted_df[wanted_df["n_overlay"] >= 0]

    rest_df = out[~out.index.isin(final_df.index)]

    # If there are not enough recipes, get random recipes with different main ingredients.
    msg_recipes = ""
    if len(final_df) < num_recipes and len(rest_df > 0):
        remaining = min(len(rest_df), num_recipes - len(final_df))

        remaining_df = rest_df.sample(remaining)
        msg_recipes = f"Number of Recipes needed to fufill constraints is {len(final_df)}, adding {remaining} random recipes. "
        final_df = pd.concat([final_df, remaining_df])

    ings_out, ings_not_full = get_ingredient_distribution(
        ingredients, final_df, ings_not_found
    )
    tax_out, taxs_not_full = get_taxonomies_distribution(
        taxonomies, final_df, taxs_not_found
    )

    prices_out, prices_not_full = get_prices_distribution(
        prices, final_df, prices_not_found
    )
    status, msg_data = get_message(
        ings_not_full,
        taxs_not_full,
        prices_not_full,
        ings_not_found,
        taxs_not_found,
        prices_not_found,
        bool(num_recipes - len(final_df)),
    )

    msg = msg_recipes + msg_data
    recipes = (
        df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(final_df.index)][
            [RECIPE_ID_COLUMN, ING_COLUMN]
        ]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    return get_output(
        company_id, week, year, status, msg, ings_out, tax_out, prices_out, recipes
    )
