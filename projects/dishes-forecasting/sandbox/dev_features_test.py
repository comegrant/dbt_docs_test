# Databricks notebook source
from databricks_env import auto_setup_env

auto_setup_env()
# COMMAND ----------
import logging  # noqa: E402

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# COMMAND ----------
from typing import Literal  # noqa: E402

from pydantic import BaseModel  # noqa: E402


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]


args = Args(
    company="GL",
    env="dev"
)
# COMMAND ----------
from dishes_forecasting.spark_context import create_spark_context  # noqa: E402

spark = create_spark_context()

# COMMAND ----------
from constants.companies import get_company_by_code  # noqa: E402

company_code = args.company
company = get_company_by_code(company_code=company_code)
company_id = company.company_id
# COMMAND ----------
from dishes_forecasting.spark_context import create_spark_context  # noqa: E402

spark = create_spark_context()
# COMMAND ----------
from databricks.feature_store import FeatureStoreClient  # noqa: E402
from dishes_forecasting.train.feature_lookup_config import feature_lookup_config_list  # noqa: E402
from dishes_forecasting.train.training_set import create_training_data_set  # noqa: E402

fs = FeatureStoreClient()
training_set, df_training_pk_target = create_training_data_set(
    env=args.env,
    company_id=company_id,
    train_config={
        "train_start_yyyyww": 202101,
        "train_end_yyyyww": 202501
    },
    fs=fs,
    spark=spark,
    feature_lookup_config_list=feature_lookup_config_list
)

df_train = training_set.load_df().toPandas()
df_train.head()
