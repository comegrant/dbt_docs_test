# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils
    dbutils: RemoteDbUtils = "" # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging
import os

dbutils.widgets.text("environment", defaultValue="")
dbutils.widgets.text("should_force_update", "false")

should_force_update = dbutils.widgets.get("should_force_update").lower() == "true"
environment = dbutils.widgets.get("environment")

assert isinstance(environment, str)
assert environment != ""

# Need to set this before importing any contracts due to env vars being accessed
# I know this is is a shit design, but it will do for now
os.environ["DATALAKE_ENV"] = environment


from data_contracts.materialize import materialize_data
from data_contracts.orders import MealboxChangesAsRating
from data_contracts.recommendations.recommendations import RecommendatedDish
from data_contracts.recommendations.store import recommendation_feature_contracts

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

logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(logging.ERROR)

store = recommendation_feature_contracts()

locations = [RecommendatedDish.location, MealboxChangesAsRating.location]

await materialize_data(store, locations, should_force_update=should_force_update)
