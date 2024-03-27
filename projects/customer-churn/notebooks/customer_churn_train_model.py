# Databricks notebook source
import sys
import os
import logging

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Insert customer_churn to path
sys.path.insert(0, '../')

# Insert lmk_utils package to path
sys.path.insert(0, "../../../packages/lmkgroup-ds-utils/")

# Insert package pydantic_argparser to path
sys.path.insert(0, "../../../packages/pydantic-argparser/")


# COMMAND ----------

try:
  COMPANY_CODE = str(getArgument("company"))
except:
  logger.warning("Failed to read company id from argument, using default")
  COMPANY_CODE = "RN"

# COMMAND ----------

from datetime import date, timedelta

# COMMAND ----------

from customer_churn.train import RunArgs, run_with_args

# COMMAND ----------

model = run_with_args(
  RunArgs(
    company=COMPANY_CODE,
    start_date=date.today() - timedelta(days=30),
    end_date=date.today(),
    local=False,
    forecast_weeks=4,
    onboarding_weeks=12,
    buy_history_churn_weeks=4,
    complaints_last_n_weeks=4,
    mlflow_tracking_uri="databricks",
    experiment_name="/Shared/customer_churn/customer_churn_v1"
  )
)
