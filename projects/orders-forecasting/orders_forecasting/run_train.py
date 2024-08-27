import logging
from typing import Literal

from constants.companies import get_company_by_code
from pydantic import BaseModel

from orders_forecasting.models.company import get_company_config_for_company
from orders_forecasting.models.features import get_features_config
from orders_forecasting.models.train import get_train_config
from orders_forecasting.train.make_dataset import load_dataset
from orders_forecasting.train.train_model import train_ensemble_model

logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    target: Literal[
        "num_total_orders",
        "num_mealbox_orders",
        "num_dishes_orders",
        "perc_dishes_orders",
    ]


def train_with_args(args: Args) -> None:
    company = get_company_by_code(args.company)
    company_config = get_company_config_for_company(company=company)
    features_config = get_features_config(company=company)
    train_config = get_train_config(
        env=args.env, target_col=args.target, company=company
    )

    # Load features
    training_set = load_dataset(
        env=args.env,
        company=company,
        features_config=features_config,
        target_col=args.target,
    )

    # Train model
    train_ensemble_model(
        company,
        training_set=training_set,
        target_col=args.target,
        env=args.env,
        train_config=train_config,
        company_config=company_config,
        is_finalize=True,
    )
