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
from orders_forecasting.run_predict import Args, run_predict_with_args  # noqa: E402

# COMMAND ----------
try:
    w = WorkspaceClient()
    env_registry = w.dbutils.widgets

except ValueError:
    env_registry = os.environ


# COMMAND ----------
company = env_registry.get("company", "GL")
env = env_registry.get("env", "dev")
num_weeks = int(env_registry.get("num_weeks", 16))
prediction_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
target = env_registry.get("target", "num_total_orders")
use_latest_model_run = True

# COMMAND ----------
if target == "all":
    for target in ["num_total_orders", "num_dishes_orders", "perc_dishes_orders"]:
        logger.info(f"Making prediction for target: {target} and company: {company}")
        args = Args(
            company=company,
            env=env,
            target=target,
            prediction_date=prediction_date,
            num_weeks=num_weeks,
            use_latest_model_run=use_latest_model_run,
        )
        predictions = run_predict_with_args(args=args)

        predictions.display()

else:
    logger.info(f"Making prediction for target: {target} and company: {company}")
    args = Args(
        company=company,
        env=env,
        target=target,
        prediction_date=prediction_date,
        num_weeks=num_weeks,
        use_latest_model_run=use_latest_model_run,
    )
    predictions = run_predict_with_args(args=args)

    predictions.display()
