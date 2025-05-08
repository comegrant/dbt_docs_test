import logging

import pandas as pd
from reci_pick.db import get_data_from_sql
from reci_pick.paths import SQL_DIR


def get_dataframes(
    **kwargs: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required_keys = ["start_yyyyww", "company_id", "env"]
    for key in required_keys:
        if key not in kwargs:
            raise KeyError(f"Missing required kwarg: {key}")

    logging.info("Starting to get dataframes.")

    try:
        logging.info("Downloading recipes...")
        df_recipes = get_data_from_sql(
            SQL_DIR / "all_recipes.sql",
            start_yyyyww=kwargs["start_yyyyww"],
            company_id=kwargs["company_id"],
            env=kwargs["env"],
        ).toPandas()

        df_recipes = df_recipes.drop_duplicates(subset="main_recipe_id")
    except Exception as e:
        logging.error(f"Error downloading recipes: {e}")
        raise
    try:
        logging.info("Downloading menus...")
        df_menu_recipes = get_data_from_sql(
            SQL_DIR / "menu_recipes.sql",
            start_yyyyww=kwargs["start_yyyyww"],
            company_id=kwargs["company_id"],
            env=kwargs["env"],
        ).toPandas()
        df_menu_recipes = df_menu_recipes.drop_duplicates(subset=["menu_year", "menu_week", "main_recipe_id"])
    except Exception as e:
        logging.error(f"Error downloading menus: {e}")
        raise
    try:
        logging.info("Downloading purchase history...")
        df_order_history = get_data_from_sql(
            SQL_DIR / "order_history.sql",
            start_yyyyww=kwargs["start_yyyyww"],
            company_id=kwargs["company_id"],
            env=kwargs["env"],
        ).toPandas()

        df_order_history["concept_combination_list"] = df_order_history["concept_combinations"].str.split(", ")
        # Potentially remove recipes that are not in the look up table
        df_order_history = df_order_history[df_order_history["main_recipe_id"].isin(df_recipes["main_recipe_id"])]
    except Exception as e:
        logging.error(f"Error downloading purchase history: {e}")
        raise
    try:
        logging.info("Downloading active users list...")
        df_active_users = get_data_from_sql(
            SQL_DIR / "active_users.sql", company_id=kwargs["company_id"], env=kwargs["env"]
        ).toPandas()
        df_active_users["concept_combination_list"] = df_active_users["concept_combinations"].str.split(", ")
    except Exception as e:
        logging.error(f"Error downloading active users list: {e}")
        raise

    try:
        logging.info("Downloading concept preferences combinations...")
        df_concept_preferences = get_data_from_sql(
            SQL_DIR / "concept_combinations.sql", company_id=kwargs["company_id"], env=kwargs["env"]
        ).toPandas()
        df_concept_preferences["concept_combination_list"] = df_concept_preferences[
            "concept_name_combinations"
        ].str.split(", ")
    except Exception as e:
        logging.error(f"Error downloading concept preference combinations: {e}")
        raise

    return df_recipes, df_menu_recipes, df_order_history, df_active_users, df_concept_preferences
