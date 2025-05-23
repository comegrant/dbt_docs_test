# Databricks notebook source

# COMMAND ----------
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()
# COMMAND ----------
import logging
import os

import pandas as pd
from catalog_connector import connection
from constants.companies import get_company_by_code
from data_contracts.sources import data_science_data_lake

# COMMAND ----------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-name",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-key",
)


# COMMAND ----------
def get_user_preferences(company_id: str) -> pd.DataFrame:
    """Get user preferences for a company."""

    df = connection.sql(
        f"""
        select
        company_id,
        negative_taste_preference_combo_id,
        negative_taste_preferences,
        negative_taste_preferences_ids,
        number_of_users
        from mlgold.menu_feedback_agreement_preferences_aggregated
        where company_id = '{company_id}'
    """
    ).toPandas()

    return df


def get_recipe_preferences() -> pd.DataFrame:
    """Get preferences for a list of main recipe ids."""

    df = connection.sql(
        """
        select
        main_recipe_id,
        recipe_main_ingredient_id,
        negative_taste_preferences,
        recipe_main_ingredient_name_english as recipe_main_ingredient_name
        from mlgold.menu_feedback_recipe_preferences
    """
    ).toPandas()

    return df


async def main() -> None:
    companies = ["LMK", "AMK", "GL", "RT"]

    for company in companies:
        company_id = get_company_by_code(company).company_id
        logger.info(f"Getting user preferences for {company}")
        user_preferences = get_user_preferences(company_id)

        logger.info("Writing user preferences to blob")
        await data_science_data_lake.config.storage.write(
            path=f"data-science/preselector/menu_feedback/user_preferences_{company}",
            content=user_preferences.to_parquet(index=False),
        )

    logger.info("Getting recipe preferences")
    recipe_preferences = get_recipe_preferences()
    logger.info("Writing recipe preferences to blob")
    await data_science_data_lake.config.storage.write(
        path="data-science/preselector/menu_feedback/recipe_preferences",
        content=recipe_preferences.to_parquet(index=False),
    )


# COMMAND ----------
await main()  # type: ignore
