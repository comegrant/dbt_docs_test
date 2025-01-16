# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
import os  # noqa: 402
API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["DATABRICKS_TOKEN"] = API_TOKEN
os.environ["DATABRICKS_HOST"] = API_ROOT
os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"
# COMMAND ----------
import mlflow  # noqa: 402
from dishes_forecasting.predict.run_predict import Args, run_predict  # noqa: 402
from dishes_forecasting.spark_context import create_spark_context  # noqa: 402

spark = create_spark_context()
is_get_params_from_workflow = True
is_running_on_databricks = True
# COMMAND ----------
# Getting parameters

dbutils.widgets.text(name='write_mode', defaultValue='append')
dbutils.widgets.text(name='env', defaultValue='dev')
dbutils.widgets.text(name='company', defaultValue='GL')


# COMMAND ----------
env = dbutils.widgets.get("env")
company = dbutils.widgets.get("company")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------
mlflow.set_tracking_uri("databricks")
args = Args(
    company=company,
    env=env,
    forecast_date="",
    write_mode=write_mode,
    is_running_on_databricks=is_running_on_databricks
)

# COMMAND ----------
df_predictions = run_predict(
    args=args,
    spark=spark
)

display(df_predictions)
