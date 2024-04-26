import logging
from datetime import UTC, date, datetime, timedelta

from constants.companies import get_company_by_code
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.config import PREP_CONFIG
from customer_churn.data.load_data import DataLoader
from customer_churn.data.snapshot import generate_snapshots_for_period

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company: str = Field("RN")
    start_date: date = Field(datetime.now(tz=UTC).date() - timedelta(days=30))
    end_date: date = Field(datetime.now(tz=UTC).date())
    save_snapshot: bool = Field(False)
    input_files: dict = Field(PREP_CONFIG["input_files"])
    snapshot_config: dict = Field(PREP_CONFIG["snapshot"])
    model_training: bool = Field(True)
    env: str = Field("dev")


def generate_snapshot(args: RunArgs) -> None:
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Load environment variables
    load_dotenv(find_dotenv())

    # Get company based on company code input
    company = get_company_by_code(args.company)

    # Fetch data
    data_loader = DataLoader(
        company,
        model_training=args.model_training,
        input_files=args.input_files,
        snapshot_config=args.snapshot_config,
    )

    # Generate snapshots
    generate_snapshots_for_period(
        data_loader=data_loader,
        company=company,
        start_date=args.start_date,
        end_date=args.end_date,
        save_snapshot=args.save_snapshot,
    )


if __name__ == "__main__":
    args = parser_for(RunArgs).parse_args()
    generate_snapshot(decode_args(args, RunArgs))
