# Databricks notebook source
import sys
import logging

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

packages = [
    "../",
    "../../../packages/data-connector/",
    "../../../packages/pydantic-argparser/",
    "../../../packages/constants/",
]

sys.path.extend(packages)

# COMMAND ----------

try:
    COMPANY_CODE = str(getArgument("company"))
except:
    logger.warning("Failed to read company code from argument, using default")
    COMPANY_CODE = "RT"

# COMMAND ----------

from customer_churn.train import RunArgs, run_with_args

# COMMAND ----------

model = run_with_args(
    RunArgs(
        company=COMPANY_CODE,
        start_date=None,  # Setting start date to none chooses one month prior to end date
        end_date=None,  # Setting end date to None chooses max date available
        forecast_weeks=4,
        onboarding_weeks=12,
        buy_history_churn_weeks=4,
        complaints_last_n_weeks=4,
        mlflow_tracking_uri="databricks",
        experiment_name="/Shared/ml_experiments/customer_churn/customer_churn_v1",
    )
)
