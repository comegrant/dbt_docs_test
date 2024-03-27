import logging
from datetime import date, timedelta
from pathlib import PosixPath

from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.features.build_features import load_training_data
from customer_churn.models.train import train_model
from customer_churn.paths import MODEL_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting predict")


class RunArgs(BaseModel):
    company: str = Field("RN")

    start_date: date = Field(date.today() - timedelta(days=30))
    end_date: date = Field(date.today())
    local: bool = Field(True)

    forecast_weeks: int = Field(4)
    onboarding_weeks: int = Field(12)
    buy_history_churn_weeks: int = Field(4)
    complaints_last_n_weeks: int = Field(4)

    write_to: PosixPath = Field(MODEL_DIR)
    model: str = Field("log_reg")
    model_version: str = Field("1.0.0")
    mlflow_tracking_uri: str | None = Field(None)


def run_with_args(args: RunArgs) -> None:
    logger.info(f"Training model with args {args}")

    # Read features data
    df_training_data = load_training_data(
        company_code=args.company,
        start_date=args.start_date,
        end_date=args.end_date,
        local=args.local,
    )

    logger.info(
        f"Features loaded for {args.company} from {args.start_date} to {args.end_date}",
    )

    write_to = args.write_to / args.model / args.model_version

    # Train model
    train_model(
        df_training_data,
        company_code=args.company,
        write_to=write_to,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )


def run() -> None:
    args = parser_for(RunArgs).parse_args()

    run_with_args(
        decode_args(args, RunArgs),
    )


if __name__ == "__main__":
    run()
