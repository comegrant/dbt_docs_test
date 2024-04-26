# Databricks notebook source
import logging
import sys

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

logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

from datetime import date, timedelta

from customer_churn.predict import RunArgs, run_with_args

# COMMAND ----------

try:
    COMPANY_CODE = str(getArgument("company"))
except:
    logger.warning("Failed to read company id from argument, using default")
    COMPANY_CODE = "RT"

# COMMAND ----------

predictions = run_with_args(
    RunArgs(
        company=COMPANY_CODE,
        start_date=date.today() - timedelta(days=300),
        end_date=date.today(),
        env="dev",
        forecast_weeks=4,
        onboarding_weeks=12,
        buy_history_churn_weeks=4,
        complaints_last_n_weeks=4,
        write_to="customer_churn_predictions",
    ),
)

# COMMAND ----------

predictions.head()
