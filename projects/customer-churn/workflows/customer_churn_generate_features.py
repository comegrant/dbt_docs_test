# Databricks notebook source
import logging
import os
from datetime import datetime, timedelta, timezone

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

from customer_churn.run_feature_generation import RunArgs, run_with_args  # noqa: E402

# COMMAND ----------

run_with_args(
    RunArgs(
        company=company,
        start_date=datetime.now(tz=timezone.utc).date() - timedelta(days=30),
        end_date=datetime.now(tz=timezone.utc).date(),
        env=env,
    )
)
