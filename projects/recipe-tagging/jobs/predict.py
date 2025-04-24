# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""

from databricks_env import setup_env

setup_env("recipe-tagging")

# COMMAND ----------

import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ROOT = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)
API_TOKEN = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
)

os.environ["DATABRICKS_TOKEN"] = API_TOKEN
os.environ["DATABRICKS_HOST"] = API_ROOT
os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-name",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-key",
)

# COMMAND ----------

import pandas as pd
from recipe_tagging.db import get_serverless_spark_session
from recipe_tagging.common import Args
from recipe_tagging.predict.predict import predict_pipeline, send_to_azure
import nltk

nltk.download("wordnet")


async def main():
    env = dbutils.widgets.get("env")
    languages = ["danish", "norwegian", "swedish"]
    spark = get_serverless_spark_session()

    dfs = []
    for language in languages:
        try:
            args = Args(language=language, env=env)
            logger.info(
                f"Starting prediction pipeline for {args.language} recipe tagging."
            )
            df = predict_pipeline(spark, args)
            dfs.append(df)
            logger.info(
                f"Finished prediction pipeline for {args.language} recipe tagging."
            )
        except Exception as e:
            logger.error(
                f"Error in prediction pipeline for {args.language} recipe tagging: {e}"
            )

    df = pd.concat(dfs)
    await send_to_azure(df, args)
    logger.info("Finished sending results to Azure storage.")


await main()
