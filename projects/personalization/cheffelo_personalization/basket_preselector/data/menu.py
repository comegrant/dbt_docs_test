import logging

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from cheffelo_personalization.basket_preselector.utils.paths import SQL_DIR

from .models import Yearweek

logger = logging.getLogger(__name__)


def get_product_informaiton(company_id: str, db: DB) -> pd.DataFrame:
    """Get product information for company

    Args:
        company_id (str): company id
        db (DB): database connection

    Returns:
        pd.DataFrame: dataframe of product information
    """
    logger.info("Get product information from database...")
    with open(SQL_DIR / "menu" / "get_product_information.sql") as f:
        return db.read_data(f.read().format(company_id=company_id))


def get_recipe_preferences(company_id: str, yearweek: Yearweek, db: DB) -> pd.DataFrame:
    """Get recipe preferences for company

    Args:
        company_id (str): company id
        yearweek (Yearweek): year week for menu
        db (DB): database connection

    Returns:
        pd.DataFrame: dataframe of recipe and preferences for a given menu week
    """
    logger.info("Get recipe preferences from database...")
    with open(SQL_DIR / "menu" / "get_recipe_preferences.sql") as f:
        return db.read_data(
            f.read().format(
                company_id=company_id, year=yearweek.year, week=yearweek.week
            )
        )


def get_menu_for_year_week(company_id: str, yearweek: Yearweek, db: DB) -> pd.DataFrame:
    """Get menu information for year week

    Args:
        company_id (str): company
        yearweek (Yearweek): year week for menu
        db (DB): database connection

    Returns:
        pd.DataFrame: dataframe of menu products for a given yearweek and company
    """
    logger.info("Get menu products from database...")
    with open(SQL_DIR / "menu" / "get_menu_for_yearweek_company.sql") as f:
        return db.read_data(
            f.read().format(
                company_id=company_id, year=yearweek.year, week=yearweek.week
            )
        )


def get_menu_for_year_week_with_preferences(
    company_id: str, yearweek: Yearweek, db: DB
) -> pd.DataFrame:
    """Get menu information for year week

    Args:
        company_id (str): company
        yearweek (Yearweek): year week for menu
        db (DB): database connection

    Returns:
        pd.DataFrame: dataframe of menu products for a given yearweek and company
    """
    logger.info("Get menu products from database...")
    with open(SQL_DIR / "menu" / "get_menu_for_yearweek_company_with_pref.sql") as f:
        return db.read_data(
            f.read().format(
                company_id=company_id, year=yearweek.year, week=yearweek.week
            )
        )


def get_menu_preference_rules(db: DB) -> pd.DataFrame:
    """Get menu preference

    Returns:
        pd.DataFrame: dataframe of preference rules and their lower and upper values
    """
    logger.info("Get menu preference rules from database...")
    with open(SQL_DIR / "menu" / "get_menu_preference_rules.sql") as f:
        df = db.read_data(f.read())

    df = df.assign(preference_id=df["preference_id"].apply(lambda x: x.lower()))
    df = df.fillna(0)
    df = df.astype({"lower_rule_value": int, "upper_rule_value": int})
    return df


def get_menu_products_with_preferences(
    company_id: str, yearweek: Yearweek, db: DB
) -> pd.DataFrame:
    """Gets the menu products with preference information

    Args:
        company_id (str): company
        yearweek (Yearweek): year week for menu
        db (DB): db connection

    Returns:
        pd.DataFrame: dataframe of menu products with preference information
    """
    # Get product information
    df_products = get_product_informaiton(company_id, db)

    # Get menu information with preferences
    df_menu_with_preferences = get_menu_for_year_week_with_preferences(
        company_id=company_id, yearweek=yearweek, db=db
    )

    # Merge together information to menu product with preferences
    df_menu_products_with_preferences = df_menu_with_preferences.merge(
        df_products, on="variation_id", how="left"
    )

    # Replace nan values for no preferences with empty bracked []
    df_menu_products_with_preferences = df_menu_products_with_preferences.assign(
        preference_ids=df_menu_products_with_preferences["preference_ids"].apply(
            lambda x: x.lower().split(", ") if isinstance(x, str) else []
        )
    )
    df_menu_products_with_preferences = df_menu_products_with_preferences.astype(
        {"variation_price": float}
    )

    return df_menu_products_with_preferences
