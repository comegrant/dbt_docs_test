# Databricks notebook source
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks_env.script import auto_setup_env

# COMMAND ----------
auto_setup_env()

# COMMAND ----------

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

from customer_churn.run_training import RunArgs, run_with_args  # noqa: E402

# COMMAND ----------

model = run_with_args(
    RunArgs(
        company=company,
        start_date=None,  # Setting start date to none chooses one month prior to end date
        end_date=None,  # Setting end date to None chooses max date available
        forecast_weeks=4,
        onboarding_weeks=12,
        buy_history_churn_weeks=4,
        complaints_last_n_weeks=4,
        env=env,
    )
)
