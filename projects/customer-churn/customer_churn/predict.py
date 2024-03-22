import asyncio
import logging
from datetime import date, timedelta

from lmkgroup_ds_utils.constants import Company
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.features.build_features import get_features
from customer_churn.models.predict import make_predictions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company: str = Field("RN")

    start_date: date = Field(date.today() - timedelta(days=30))
    end_date: date = Field(date.today())

    forecast_weeks: int = Field(4)
    onboarding_weeks: int = Field(12)
    buy_history_churn_weeks: int = Field(4)
    complaints_last_n_weeks: int = Field(4)

    write_to: str = Field(f"data/customer_churn/{date.today().isoformat()}")


async def run_with_args(args: RunArgs) -> None:
    logger.info(args)
    logger.info(Company.get_id_from_name(args.company))
    if args.company:
        features = get_features(
            company_id=Company.get_id_from_name(args.company),
            start_date=args.start_date,
            end_date=args.end_date,
        )
        if logger:
            logger.info(f"Start processing data for {args.company}")
    else:
        raise ValueError("Unable to create features")

    return await make_predictions(features)


def run() -> None:
    args = parser_for(RunArgs).parse_args()

    asyncio.run(
        run_with_args(
            decode_args(args, RunArgs),
        ),
    )


if __name__ == "__main__":
    run()
