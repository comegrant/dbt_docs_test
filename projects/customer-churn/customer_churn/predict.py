import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from constants.companies import get_company_by_code
from data_connector.databricks_connector import (
    is_running_databricks,
    save_dataframe_to_table,
)
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.data.preprocess import Preprocessor
from customer_churn.features.build_features import get_features
from customer_churn.models.predict import make_predictions
from customer_churn.paths import OUTPUT_DIR
from customer_churn.utils.file import (
    save_predictions_locally,
)

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company: str = Field("RN")

    start_date: date = Field(date.today() - timedelta(days=30))
    end_date: date = Field(date.today())
    env: str = Field("dev")

    forecast_weeks: int = Field(4)
    onboarding_weeks: int = Field(12)
    buy_history_churn_weeks: int = Field(4)
    complaints_last_n_weeks: int = Field(4)

    write_to: Path = Field(OUTPUT_DIR / f"{date.today().isoformat()}")


def run_with_args(args: RunArgs) -> None:
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    # Load environment variables
    load_dotenv(find_dotenv())

    logger.info(f"Running predict with args {args}")

    # Get features
    company = get_company_by_code(args.company)
    if args.company:
        features = get_features(
            company=company,
            start_date=args.start_date,
            end_date=args.end_date,
            env=args.env,
        )
        if logger:
            logger.info(f"Start processing data for {args.company}")
    else:
        raise ValueError("Unable to create features")

    # Preprocess features for prediction
    logger.info("Preprocessing features for prediction...")
    features = Preprocessor().prep_prediction(df=features)

    # Make predictions
    predictions = make_predictions(
        features,
        company=company,
        forecast_weeks=args.forecast_weeks,
    )

    predictions["run_id"] = uuid.uuid4()
    predictions["run_date"] = datetime.now(tz=UTC).strftime(
        "%Y-%m-%d %H:%M:%S",
    )
    # predictions["snapshot_based_on"] = snapshot_read
    predictions["company_id"] = company.company_id

    # Save predictions
    if is_running_databricks():
        logger.info(f"Writing predictions to {args.write_to}")
        save_dataframe_to_table(
            predictions=predictions,
            table_name=args.write_to,
            catalog_name="dev",
            schema="mltesting",
            mode="overwrite",
        )
    else:
        save_predictions_locally(
            predictions=predictions,
            local_file_dir=OUTPUT_DIR / args.write_to,
            local_filename="predictions.csv",
        )

    logger.info(predictions)

    return predictions


def run() -> None:
    args = parser_for(RunArgs).parse_args()

    run_with_args(
        decode_args(args, RunArgs),
    )


if __name__ == "__main__":
    run()
