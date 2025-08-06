import logging

import pandas as pd
from reci_pick.db import get_data_from_sql
from reci_pick.paths import SQL_DIR

logger = logging.getLogger(__name__)


def get_dataframes(
    **kwargs: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required_keys = ["start_yyyyww", "company_id"]
    for key in required_keys:
        if key not in kwargs:
            raise KeyError(f"Missing required kwarg: {key}")

    logger.info("Starting to get dataframes.")
    try:
        logger.info("Downloading recipes...")
        df_recipes = get_data_from_sql(
            SQL_DIR / "reci_pick_all_recipes.sql",
            start_yyyyww=kwargs["start_yyyyww"],
            company_id=kwargs["company_id"],
        ).toPandas()
        df_recipes = df_recipes.drop_duplicates(subset="main_recipe_id")
    except Exception as e:
        logger.error(f"Error downloading recipes: {e}")
        raise
    try:
        logger.info("Downloading menus...")
        df_menu_recipes = get_data_from_sql(
            SQL_DIR / "reci_pick_menu_recipes.sql",
            start_yyyyww=kwargs["start_yyyyww"],
            company_id=kwargs["company_id"],
        ).toPandas()
        df_menu_recipes = df_menu_recipes[df_menu_recipes["main_recipe_id"].isin(df_recipes["main_recipe_id"])]
    except Exception as e:
        logger.error(f"Error downloading menus: {e}")
        raise

    try:
        logger.info("Downloading purchase history...")
        df_order_history = get_data_from_sql(
            SQL_DIR / "reci_pick_order_history.sql",
            start_yyyyww=kwargs["start_yyyyww"],
            company_id=kwargs["company_id"],
        ).toPandas()

        df_order_history["concept_combination_list"] = (
            df_order_history["concept_combinations"].fillna("").str.split(", ").tolist()
        )
        # Potentially remove recipes that are not in the look up table
        df_order_history = df_order_history[df_order_history["main_recipe_id"].isin(df_recipes["main_recipe_id"])]
    except Exception as e:
        logging.error(f"Error downloading purchase history: {e}")
        raise

    return df_recipes, df_menu_recipes, df_order_history
