from typing import Literal

import pandas as pd
from constants.companies import get_company_by_code
from pydantic import BaseModel
from pyspark.sql import SparkSession
from sklearn.pipeline import Pipeline

from databricks.feature_store import FeatureStoreClient
from dishes_forecasting.train.configs.feature_lookup_config import feature_lookup_config_list
from dishes_forecasting.train.configs.hyper_params import load_hyperparams
from dishes_forecasting.train.configs.train_configs import get_training_configs
from dishes_forecasting.train.train_pipeline import train_model
from dishes_forecasting.train.training_set import create_training_set


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    is_running_on_databricks: bool


def run_train(args: Args, spark: SparkSession) -> tuple[Pipeline, pd.Series, pd.Series, pd.Series, pd.Series]:
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

    params_lgb, params_rf, params_xgb = load_hyperparams(company=args.company)
    custom_pipeline, X_train, X_test, y_train, y_test, _ = train_model(  # noqa
        training_set=training_set,
        params_lgb=params_lgb,
        params_rf=params_rf,
        params_xgb=params_xgb,
        is_running_on_databricks=args.is_running_on_databricks,
        env="dev",
        spark=spark,
        train_config=train_config,
        company=company
    )
    return custom_pipeline, X_train, X_test, y_train, y_test
