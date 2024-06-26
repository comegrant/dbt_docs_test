import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from constants.companies import Company, get_company_by_code
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.features.build_features import generate_features
from customer_churn.models.features import get_features_config

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company: str = Field("RN")
    start_date: date = Field(datetime.now(tz=timezone.utc).date() - timedelta(days=30))
    end_date: date = Field(datetime.now(tz=timezone.utc).date())
    env: str = Field("dev")


def run_with_args(args: RunArgs) -> None:
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    features_config = get_features_config()

    # Get company based on company code input
    company = get_company_by_code(args.company)

    # Generate snapshots for period
    snapshot_dates = get_snapshot_dates_based_on_company(
        args.start_date, args.end_date, company
    )

    for snapshot_date in snapshot_dates:
        logger.info("Generating features for %s", snapshot_date)
        generate_features(
            env=args.env,
            company=company,
            snapshot_date=snapshot_date,
            features_config=features_config,
        )

        logger.info("Features generated successfully!")


def get_snapshot_dates_based_on_company(
    snapshot_start_date: str,
    snapshot_end_date: str,
    company: Company,
    cut_off_friday: int = 4,
) -> list:
    # Get snapshot dates based on company orders cutoff date
    if company.cut_off_week_day == cut_off_friday:
        return pd.date_range(
            start=snapshot_start_date,
            end=snapshot_end_date,
            freq="W-FRI",  # RN orders cutoff Thursday
            tz="UTC",
        )
    else:
        return pd.date_range(
            start=snapshot_start_date,
            end=snapshot_end_date,
            freq="W-WED",  # All others orders cutoff Tuesday
            tz="UTC",
        )


if __name__ == "__main__":
    args = parser_for(RunArgs).parse_args()
    generate_features(decode_args(args, RunArgs))
