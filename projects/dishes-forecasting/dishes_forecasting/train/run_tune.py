from typing import Literal


from constants.companies import get_company_by_code
from pydantic import BaseModel

from dishes_forecasting.train.configs.feature_lookup_config import feature_lookup_config_list

from dishes_forecasting.train.configs.train_configs import get_training_configs
from dishes_forecasting.train.training_set import create_training_set
from dishes_forecasting.train.tune import tune_pipeline


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    is_running_on_databricks: bool


def run_tune(args: Args) -> tuple[dict, float]:
    company = get_company_by_code(args.company)
    company_id = company.company_id
    train_config = get_training_configs(company_code=args.company)
    training_set, _ = create_training_set(
        company_id=company_id,
        train_config=train_config,
        feature_lookup_config_list=feature_lookup_config_list,
        is_drop_ignored_columns=True,
    )
    tune_pipeline(company=company, env=args.env, train_config=train_config, training_set=training_set)
