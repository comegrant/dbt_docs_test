from typing import Literal

from constants.companies import get_company_by_code
from databricks import feature_store
from pydantic import BaseModel

from dishes_forecasting.paths import CONFIG_DIR
from dishes_forecasting.train.model import train_model
from dishes_forecasting.train.training_set import create_training_data_set
from dishes_forecasting.utils import read_yaml


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]


def run_train(args: Args) -> None:
    company = get_company_by_code(args.company)
    company_id = company.company_id

    train_configs = read_yaml("train.yml", directory=CONFIG_DIR)
    train_config = train_configs[args.company]
    fs = feature_store.FeatureStoreClient()
    training_set, df_orders = create_training_data_set(
        env=args.env, company_id=company_id, train_config=train_config, fs=fs
    )

    model_config = train_config["model_config"]
    mlflow_config = train_config["mlflow_config"]
    train_model(
        training_set=training_set,
        df_orders=df_orders,
        model_config=model_config,
        mlflow_config=mlflow_config,
        env=args.env,
        company_code=args.company,
    )
