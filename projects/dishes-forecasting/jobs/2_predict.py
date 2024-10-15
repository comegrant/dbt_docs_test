# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
import mlflow
from dishes_forecasting.run_predict import Args, run_predict
from dishes_forecasting.spark_context import create_spark_context

spark = create_spark_context()
is_get_params_from_workflow = True
is_running_on_databricks = True
# COMMAND ----------
# Getting parameters
if is_get_params_from_workflow:
    env = dbutils.widgets.get("env")
    company = dbutils.widgets.get("company")
else:
    env = "dev"
    company = "GL"

# COMMAND ----------
mlflow.set_tracking_uri("databricks")
args = Args(
    company=company,
    env=env,
    forecast_date="",
    is_running_on_databricks=is_running_on_databricks
)

# COMMAND ----------
df_predictions = run_predict(
    args=args,
    spark=spark
)

display(df_predictions)
