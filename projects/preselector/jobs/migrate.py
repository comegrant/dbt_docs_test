import asyncio
import logging
from contextlib import suppress
from typing import Literal

from pydantic import BaseModel
from pydantic_argparser import parse_args

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    env: Literal["test", "prod", "dev"]


async def add_recipes_column() -> None:
    from data_contracts.preselector.store import SuccessfulPreselectorOutput
    from data_contracts.sources import databricks_catalog
    from data_contracts.unity_catalog import UCTableSource

    success_source = SuccessfulPreselectorOutput.metadata.source
    assert isinstance(success_source, UCTableSource)

    batch_source = databricks_catalog.schema("mloutputs").table("preselector_batch")

    spark = batch_source.config.connection()

    recipe_schema = """recipes ARRAY<STRUCT<
        main_recipe_id: INT,
        variation_id: STRING,
        compliancy: INT
    >>"""

    spark.sql(f"""ALTER TABLE {batch_source.table.identifier()}
    ADD COLUMNS ({recipe_schema});""")

    spark.sql(f"""ALTER TABLE {success_source.table.identifier()}
    ADD COLUMNS ({recipe_schema});""")

async def migrate() -> None:
    await add_recipes_column()

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
