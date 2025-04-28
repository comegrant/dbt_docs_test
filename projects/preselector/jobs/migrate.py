import asyncio
import logging
from contextlib import suppress
from typing import Literal

from aligned.feature_view.feature_view import FeatureViewWrapper
from data_contracts.preselector.store import FailedPreselectorOutput, Preselector, SuccessfulPreselectorOutput
from data_contracts.sources import ml_outputs
from data_contracts.unity_catalog import UCTableSource
from pydantic import BaseModel
from pydantic_argparser import parse_args

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    env: Literal["test", "prod", "dev"]


async def migrate_source(source: UCTableSource, view: FeatureViewWrapper) -> None:
    from data_contracts.unity_catalog import pyspark_schema_changes

    table_identifier = source.table.identifier()
    spark = source.config.connection()
    if not spark.catalog.tableExists(table_identifier):
        logger.info(f"Did not find table '{table_identifier}'. Will not migrate.")
        return

    df = spark.read.table(table_identifier)

    change = pyspark_schema_changes(df.schema, view.request.spark_schema())

    if not change.has_changes:
        logger.info(
            f"Everything is up to date. Skipping migration of source '{table_identifier}'"
            f" to the expected schema in view '{view.location}'"
        )
        return

    spark_sql = change.to_spark_sql(table_identifier)

    commands = spark_sql.split(";")
    for command in commands:
        if command is None or command.strip() == "":
            continue
        logger.info(f"Running sql: '{command}'")
        spark.sql(command)


async def migrate() -> None:
    # await migrate_source(ml_outputs.table("preselector_batch"), Preselector)
    await migrate_source(ml_outputs.table("preselector_successful_realtime_output"), SuccessfulPreselectorOutput)
    await migrate_source(ml_outputs.table("preselector_failed_realtime_output"), FailedPreselectorOutput)
    await migrate_source(ml_outputs.table("preselector_validate"), Preselector)


async def main() -> None:
    import os

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("azure").setLevel(logging.ERROR)

    args = parse_args(RunArgs)

    os.environ["UC_ENV"] = args.env
    os.environ["DATALAKE_ENV"] = args.env

    await migrate()


if __name__ == "__main__":
    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main())
