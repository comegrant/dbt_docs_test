# Databricks notebook source
import logging
import os
from datetime import datetime, timezone

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks_env.script import auto_setup_env

auto_setup_env()

from orders_forecasting.run_evaluate import (  # noqa: E402
    Args,
    evaluate_with_args,
)

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
now = datetime.now(tz=timezone.utc).date()

last_week = now - pd.DateOffset(weeks=1)
year = last_week.year
week = last_week.week

# COMMAND ----------
logger.info(f"Evaluating model for company: {company}")
args = Args(
    company=company,
    env=env,
    year=year,
    week=week,
)
evaluate_with_args(args=args)
