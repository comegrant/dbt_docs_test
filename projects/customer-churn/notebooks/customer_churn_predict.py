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

logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

from pathlib import Path
from datetime import date, timedelta

from lmkgroup_ds_utils.constants import Companies
from customer_churn.predict import run_with_args, RunArgs

# COMMAND ----------

try:
  COMPANY_CODE = str(getArgument("company"))
except:
  logger.warning("Failed to read company id from argument, using default")
  COMPANY_CODE = "RN"

# COMMAND ----------

predictions = run_with_args(
  RunArgs(
    company=COMPANY_CODE,
    start_date = date.today() - timedelta(days=30),
    end_date = date.today(),
    local = False,
    forecast_weeks = 4,
    onboarding_weeks = 12,
    buy_history_churn_weeks = 4,
    complaints_last_n_weeks = 4,
    write_to=Path(f"customer-churn/{COMPANY_CODE}/output/predictions/")
  )
)

# COMMAND ----------

predictions.head()
