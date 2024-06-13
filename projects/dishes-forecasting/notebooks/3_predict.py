# Databricks notebook source
import logging


from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
from dishes_forecasting.run_predict import Args, run_predict
import mlflow

mlflow.set_tracking_uri("databricks")
args = Args(company="GL", env="dev", prediction_date="2024-02-01", num_weeks=11)
score_df = run_predict(args=args)
display(score_df)
