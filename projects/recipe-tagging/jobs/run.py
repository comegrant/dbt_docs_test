# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils  # type: ignore
    dbutils: RemoteDbUtils = "" # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
from recipe_tagging.main import RunArgs, run

run(RunArgs())  # type: ignore
