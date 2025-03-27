# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
from analytics_api.main import RunArgs, run

await run(RunArgs())
