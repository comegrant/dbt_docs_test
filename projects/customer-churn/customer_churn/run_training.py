import logging
from datetime import date
from pathlib import PosixPath

from constants.companies import get_company_by_code
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.models.features import get_features_config
from customer_churn.models.mlflow import get_mlflow_config
from customer_churn.models.train import get_train_config
from customer_churn.paths import MODEL_DIR
from customer_churn.train.make_dataset import load_dataset
from customer_churn.train.train_model import train_ensemble_model

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company: str = Field("RT")

    start_date: date | None = Field(None)
    end_date: date | None = Field(None)

    forecast_weeks: int = Field(4)
    onboarding_weeks: int = Field(12)
    buy_history_churn_weeks: int = Field(4)
    complaints_last_n_weeks: int = Field(4)

    write_to: PosixPath = Field(MODEL_DIR)
    model: str = Field("log_reg")
    env: str = Field("dev")
    mlflow_model_version: str = Field("1")


def run_with_args(args: RunArgs) -> None:
    """Run training with arguments

    Args:
        args (RunArgs): _description_
    """
    logger.info(f"Training model with args {args}")
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    feature_config = get_features_config()
    train_config = get_train_config()
    mlflow_config = get_mlflow_config(env=args.env)

    # Load environment variables
    load_dotenv(find_dotenv())

    company = get_company_by_code(args.company)

    # Read features data
    df_training_set = load_dataset(
        env=args.env,
        company=company,
        feature_config=feature_config,
        train_config=train_config,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    # Train model
    train_ensemble_model(
        df_training_set=df_training_set,
        company_code=args.company,
        train_config=train_config,
        mlflow_config=mlflow_config,
        env=args.env,
    )


def run() -> None:
    args = parser_for(RunArgs).parse_args()

    run_with_args(
        decode_args(args, RunArgs),
    )


if __name__ == "__main__":
    run()
