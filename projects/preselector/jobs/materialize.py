# Databricks notebook source
# COMMAND ----------

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging
import os

from data_contracts.preselector.store import (
    Preselector,
    RecipePreferences,
)
from data_contracts.recommendations.store import recommendation_feature_contracts
from preselector.materialize import materialize_data

os.environ["ADB_CONNECTION"] = dbutils.secrets.get(
    scope="auth_common",
    key="analyticsDb-connectionString",
).replace("ODBC Driver 17", "ODBC Driver 18")

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-name",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-key",
)

dbutils.widgets.text("should_force_update", "0")
should_force_update = bool(dbutils.widgets.get("should_force_update"))


logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(logging.ERROR)

store = recommendation_feature_contracts()
store.add_feature_view(Preselector)
store.add_feature_view(RecipePreferences)

locations = list(Preselector.query().view.source.depends_on())

await materialize_data(store, locations, do_freshness_check=not should_force_update)
