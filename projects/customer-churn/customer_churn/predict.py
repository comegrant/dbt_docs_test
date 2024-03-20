import logging
from datetime import UTC, date, datetime

from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.features.build_features import get_features
from customer_churn.models.predict import make_predictions

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company_id: str = Field("6A2D0B60-84D6-4830-9945-58D518D27AC2")

    log_file_dir: str | None = Field(None)
    cache_location: str | None = Field(None)

    start_date: datetime | None = Field(None)
    end_date: datetime | None = Field(None)

    forecast_weeks: int = Field(4)
    onboarding_weeks: int = Field(12)
    buy_history_churn_weeks: int = Field(4)
    complaints_last_n_weeks: int = Field(4)

    write_to: str = Field(f"data/customer_churn/{date.today().isoformat()}")


def setup_logger(log_dir: str | None) -> None:
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
        logging.ERROR,
    )
    now = datetime.now(tz=UTC).strftime("%Y-%m-%d_%H:%M:%S")
    if log_dir:
        logging.basicConfig(level=logging.INFO, filename=f"{log_dir}/{now}")
    else:
        logging.basicConfig(level=logging.INFO)


async def run_with_args(args: RunArgs) -> None:
    setup_logger(args.log_file_dir)

    if args.company_id:
        features = get_features(
            company_id=args.company_id,
            start_date=args.start_date,
            end_date=args.end_date,
            forecast_weeks=args.forecast_weeks,
            onboarding_weeks=args.onboarding_weeks,
            buy_history_churn_weeks=args.buy_history_churn_weeks,
            complaints_last_n_weeks=args.complaints_last_n_weeks,
        )
        if logger:
            logger.info(f"Start processing data for {args.company_id}")
    else:
        raise ValueError("Unable to create features")

    return await make_predictions(features)


def run() -> None:
    args = parser_for(RunArgs).parse_args()

    run_with_args(
        decode_args(args, RunArgs),
    )


if __name__ == "__main__":
    run()
