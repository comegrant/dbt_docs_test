# Databricks notebook source
import logging
import os
from datetime import date

from databricks.sdk import WorkspaceClient
from databricks_env.script import auto_setup_env

# COMMAND ----------

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auto_setup_env()

# COMMAND ----------

try:
    w = WorkspaceClient()
    env_registry = w.dbutils.widgets

except ValueError:
    env_registry = os.environ

# COMMAND ----------

company = env_registry.get("company", "GL")
env = env_registry.get("env", "dev")

# COMMAND ----------

from customer_churn.run_predict import RunArgs, run_with_args  # noqa: E402

predictions = run_with_args(
    RunArgs(
        company=company,
        prediction_date=date.today(),
        env=env,
    ),
)
