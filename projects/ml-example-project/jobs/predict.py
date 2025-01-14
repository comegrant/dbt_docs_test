# Databricks notebook source
import logging
import os
from typing import TYPE_CHECKING, Literal, cast

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore

# COMMAND ----------
API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()  # type: ignore
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # type: ignore

os.environ["DATABRICKS_TOKEN"] = API_TOKEN
os.environ["DATABRICKS_HOST"] = API_ROOT
os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

# COMMAND ----------
from distutils.util import strtobool

from ml_example_project.db import get_spark_session
from ml_example_project.predict.predict import Args

env = cast(Literal["dev", "test", "prod"], dbutils.widgets.get("env"))
company = cast(Literal["LMK", "AMK", "GL", "RT"], dbutils.widgets.get("company"))
is_run_on_databricks = bool(strtobool(dbutils.widgets.get("is_run_on_databricks")))
predict_start_yyyyww = int(dbutils.widgets.get("predict_start_yyyyww"))
predict_end_yyyyww = int(dbutils.widgets.get("predict_end_yyyyww"))


spark = get_spark_session()
args = Args(
    company=company,
    predict_start_yyyyww=predict_start_yyyyww,
    predict_end_yyyyww=predict_end_yyyyww,
    env=env,
    is_run_on_databricks=is_run_on_databricks,
)

# COMMAND ----------
from ml_example_project.predict.predict import make_predictions

df_predict = make_predictions(args=args, spark=spark)
display(df_predict)  # type: ignore
