# Databricks notebook source
import logging
from typing import TYPE_CHECKING, Literal, cast

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore


# COMMAND ----------
from distutils.util import strtobool

from ml_example_project.db import get_spark_session
from ml_example_project.train.train import Args

env = cast(Literal["dev", "test", "prod"], dbutils.widgets.get("env"))
company = cast(Literal["LMK", "AMK", "GL", "RT"], dbutils.widgets.get("company"))
is_run_on_databricks = bool(strtobool(dbutils.widgets.get("is_run_on_databricks")))


spark = get_spark_session()
args = Args(company=company, env=env, is_run_on_databricks=is_run_on_databricks, is_use_feature_store=True)

# COMMAND ----------
from ml_example_project.train.train import train_model

train_model(args=args, spark=spark)
