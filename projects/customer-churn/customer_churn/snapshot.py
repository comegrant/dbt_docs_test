import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from lmkgroup_ds_utils.constants import Companies
from lmkgroup_ds_utils.db.connector import DB
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from customer_churn.config import PREP_CONFIG
from customer_churn.data.load_data import DataLoader
from customer_churn.data.snapshot import generate_snapshots_for_period
from customer_churn.paths import DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company: str = Field("RN")

    start_date: date = Field(datetime.now(tz=UTC).date() - timedelta(days=30))
    end_date: date = Field(datetime.now(tz=UTC).date())

    save_snapshot: bool = Field(False)
    output_dir: Path = Field(DATA_DIR / "snapshot")
    output_file_prefix: str = Field("snapshot_")

    input_files: dict = Field(PREP_CONFIG["input_files"])
    snapshot_config: dict = Field(PREP_CONFIG["snapshot"])

    db_env: str = Field("prod")
    db_connection_string: str = Field("")

    local: bool = Field(True)


def generate_snapshot(args: RunArgs) -> None:
    adb = DB(db_name="analytics_db", env=args.db_env, local=args.local)
    postgres_db = DB(db_name="postgres_db", env=args.db_env, local=args.local)

    data_loader = DataLoader(
        Companies.get_id_from_name(args.company),
        input_files=args.input_files,
        snapshot_config=args.snapshot_config,
        adb=adb,
        postgres_db=postgres_db,
    )

    generate_snapshots_for_period(
        data_loader=data_loader,
        company_id=args.company,
        start_date=args.start_date,
        end_date=args.end_date,
        save_snapshot=args.save_snapshot,
        output_dir=args.output_dir,
        output_file_prefix=args.output_file_prefix,
    )


if __name__ == "__main__":
    args = parser_for(RunArgs).parse_args()
    generate_snapshot(decode_args(args, RunArgs))
