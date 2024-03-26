import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lmkgroup_ds_utils.azure.storage import BlobConnector
from lmkgroup_ds_utils.constants import Companies
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.data.preprocess import Preprocessor
from customer_churn.features.build_features import get_features
from customer_churn.models.predict import make_predictions
from customer_churn.paths import OUTPUT_DIR
from customer_churn.utils.file import (
    save_and_upload_results_to_datalake,
    save_predictions_locally,
)

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

    write_to: Path = Field(OUTPUT_DIR / f"{date.today().isoformat()}")


def run_with_args(args: RunArgs) -> None:
    logger.info(f"Running predict with args {args}")
    company_id = Companies.get_id_from_code(args.company)
    if args.company:
        features, snapshot_read = get_features(
            company_id=company_id,
            start_date=args.start_date,
            end_date=args.end_date,
            local=args.local,
        )
        if logger:
            logger.info(f"Start processing data for {args.company}")
    else:
        raise ValueError("Unable to create features")

    features = Preprocessor().prep_prediction(df=features)

    predictions = make_predictions(
        features,
        company_name=args.company,
        forecast_weeks=args.forecast_weeks,
    )

    predictions["run_id"] = uuid.uuid4()
    predictions["run_date"] = datetime.now(tz=UTC).strftime(
        "%Y-%m-%d %H:%M:%S",
    )
    predictions["snapshot_based_on"] = snapshot_read
    predictions["company_id"] = company_id

    if args.local:
        save_predictions_locally(
            predictions=predictions,
            local_file_dir=args.write_to,
            local_filename="predictions.csv",
        )
    else:
        logger.info(f"Writing predictions to {args.write_to}")
        local_path = "/tmp/" / args.write_to
        save_and_upload_results_to_datalake(
            datalake_handler=BlobConnector(local=False),
            predictions=predictions,
            filename=f"predictions_{date.today().isoformat()}.csv",
            local_path=local_path,
            dl_path=args.write_to,
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
