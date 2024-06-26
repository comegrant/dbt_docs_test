# Databricks notebook source
import logging
import os
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks_env.script import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
from orders_forecasting.run_generate_final_predictions import (  # noqa: E402
    Args,
    run_generate_final_predictions_with_args,
)

# COMMAND ----------
try:
    w = WorkspaceClient()
    env_registry = w.dbutils.widgets

except ValueError:
    env_registry = os.environ


# COMMAND ----------
company = env_registry.get("company", "GL")
env = env_registry.get("env", "dev")
prediction_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

# COMMAND ----------

logger.info(f"Generating final predictions for company {company}")
args = Args(
    company=company,
    env=env,
    prediction_date=prediction_date,
)
run_generate_final_predictions_with_args(args=args)
