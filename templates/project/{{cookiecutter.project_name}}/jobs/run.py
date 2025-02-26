# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils
    dbutils: RemoteDbUtils = "" # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()
# NB!!! Do not import project specific dependencies above this line!

# COMMAND ----------
from {{cookiecutter.module_name}}.main import run, RunArgs

await run(RunArgs())
