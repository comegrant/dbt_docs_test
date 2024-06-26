# Databricks notebook source
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks_env.script import auto_setup_env

auto_setup_env()

from orders_forecasting.run_train import Args, train_with_args  # noqa: E402

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    w = WorkspaceClient()
    env_registry = w.dbutils.widgets

except ValueError:
    env_registry = os.environ


# COMMAND ----------
company = env_registry.get("company", "GL")
env = env_registry.get("env", "dev")
target = env_registry.get("target", "num_total_orders")

# COMMAND ----------
if target == "all":
    for target in ["num_total_orders", "num_dishes_orders", "perc_dishes_orders"]:
        logger.info(f"Training model for target: {target} and company: {company}")
        args = Args(
            company=company,
            env=env,
            target=target,
        )
        train_with_args(args=args)
else:
    logger.info(f"Training model for target: {target} and company: {company}")
    args = Args(
        company=company,
        env=env,
        target=target,
    )
    train_with_args(args=args)
