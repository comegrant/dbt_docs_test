from typing import Literal


from constants.companies import get_company_by_code
from pydantic import BaseModel
from pyspark.sql import SparkSession


from databricks.feature_store import FeatureStoreClient
from dishes_forecasting.train.configs.feature_lookup_config import feature_lookup_config_list

from dishes_forecasting.train.configs.train_configs import get_training_configs
from dishes_forecasting.train.training_set import create_training_set
from dishes_forecasting.train.tune import grid_search_params


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    is_running_on_databricks: bool


def run_tune(args: Args, spark: SparkSession) -> tuple[dict, float]:
    company = get_company_by_code(args.company)
    company_id = company.company_id
    train_config = get_training_configs(company_code=args.company)
    fs = FeatureStoreClient()
    training_set, _ = create_training_set(
        is_use_feature_store=False,  # set to false for now for easy testing
        fs=fs,
        env=args.env,
        company_id=company_id,
        train_config=train_config,
        spark=spark,
        feature_lookup_config_list=feature_lookup_config_list
    )
    grid_search_params(
        company=company,
        env=args.env,
        spark=spark,
        train_config=train_config,
        training_set=training_set,
        # n_trials=50
    )
