# Databricks notebook source
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks_env.script import auto_setup_env

auto_setup_env()

# COMMAND ----------
from orders_forecasting.run_generate_features import (  # noqa: E402
    Args,
    generate_features,
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

logger.info("Generating features for company: %s", company)
args = Args(company=company, env="dev")
generate_features(args=args)
