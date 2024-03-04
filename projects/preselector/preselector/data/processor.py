import pandas as pd

from preselector.data.customers import get_customers_with_mealbox
from preselector.utils.constants import (
    FLEX_PRODUCT_TYPE_ID,
    MEALBOX_PRODUCT_TYPE_ID,
)


def parse_recipe_information(recipe_information: list) -> pd.DataFrame:
    """Parses recipe information from PIM

    Args:
        recipe_information (_type_): List of recipes

    Returns:
        pd.DataFrame: _description_
    """
    columns = ["variation_id", "preferences", "main_recipe_ids"]
    recipes = pd.DataFrame(columns=columns)

    list_recipes = [recipes]
    for portions in recipe_information:
        week_recipes = pd.DataFrame(portions["recipes"])
        list_recipes.append(week_recipes)

    df_recipes = pd.concat(list_recipes)[columns]
    return df_recipes


def process_api_data(
    agreements: list,
    product_information: list,
    recipe_information: list,
    recommendation_score: list | None,
    preference_rules: list,
    run_config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_products = pd.DataFrame(product_information)
    df_recipes = parse_recipe_information(recipe_information=recipe_information)
    df_preference_rules = pd.DataFrame(preference_rules)

    df_recommendation_score = pd.DataFrame()
    if recommendation_score:
        df_recommendation_score = pd.DataFrame(recommendation_score)

    df_menu = df_recipes.merge(df_products, on="variation_id", how="left").rename(
        columns={
            "preferences": "preference_ids",
            "product_id": "product_id",
            "name": "variation",
            "portions": "variation_portions",
            "meals": "variation_meals",
            "price": "variation_price",
            "main_recipe_ids": "main_recipe_id",
        },
    )
    df_menu = df_menu.assign(
        product_type_id=df_menu["product_type_id"].apply(lambda s: s.upper()),
    )

    df_customers = pd.DataFrame(agreements)

    df_customers, df_flex_products, df_recommendation_score = process_raw_data(
        df_customers=df_customers,
        df_menu=df_menu,
        df_recommendation_score=df_recommendation_score,
        run_config=run_config,
    )

    return df_customers, df_flex_products, df_recommendation_score, df_preference_rules


def process_raw_data(
    df_customers: pd.DataFrame,
    df_menu: pd.DataFrame,
    df_recommendation_score: pd.DataFrame,
    run_config: dict | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Merge productinformation to customer subscription
    df_mealboxes = df_menu[df_menu["product_type_id"] == MEALBOX_PRODUCT_TYPE_ID.upper()][
        [
            "product_id",
            "variation_id",
            "variation_portions",
            "variation_meals",
            "variation_price",
        ]
    ].drop_duplicates()
    df_flex_products = df_menu[df_menu["product_type_id"] == FLEX_PRODUCT_TYPE_ID.upper()].explode(
        "main_recipe_id",
    )

    df_customers = get_customers_with_mealbox(df_customers, df_mealboxes)

    return df_customers, df_flex_products, df_recommendation_score
