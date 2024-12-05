# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------
from ml_example_project.db import get_spark_session # noqa
from ml_example_project.predict.predict import Args

env = dbutils.widgets.get("env")
company = dbutils.widgets.get("company")
is_run_on_databricks = dbutils.widgets.get("is_run_on_databricks")
predict_start_yyyyww = dbutils.widgets.get("predict_start_yyyyww")
predict_end_yyyyww = dbutils.widgets.get("predict_end_yyyyww")


spark = get_spark_session()
args = Args(
    company=company,
    predict_start_yyyyww=predict_start_yyyyww,
    predict_end_yyyyww=predict_end_yyyyww,
    env=env,
    is_run_on_databricks=is_run_on_databricks
)

# COMMAND ----------
from ml_example_project.predict.predict import make_predictions  # noqa
df_predict = make_predictions(
    args=args,
    spark=spark
)
display(df_predict)
