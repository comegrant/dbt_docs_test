import asyncio
import logging
from contextlib import suppress
from typing import Literal

from aligned.schemas.feature import StaticFeatureTags
from data_contracts.preselector.basket_features import BasketFeatures
from pydantic import BaseModel
from pydantic_argparser import parse_args

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    env: Literal["test", "prod", "dev"]


async def migrate_batch_error() -> None:
    import polars as pl
    from data_contracts.preselector.store import Preselector
    from data_contracts.sources import databricks_catalog

    source = databricks_catalog.schema("mloutputs").table("preselector_batch")
    source = source.overwrite_schema()

    df = await source.all_columns().to_pandas()
    df["taste_preferences"] = df["taste_preferences"].astype(str)

    error_features = [
        feat.name for feat
        in BasketFeatures.query().request.all_returned_features
        if StaticFeatureTags.is_entity not in (feat.tags or [])
    ]
    error_vector_type = pl.Struct({
        feat: pl.Float64
        for feat in error_features
    })

    new_df = pl.from_pandas(df).with_columns(
        pl.col("error_vector").cast(error_vector_type)
    )

    await Preselector.query().store.update_source_for(
        Preselector.location,
        source
    ).overwrite(
        Preselector.location,
        new_df
    )

async def migrate() -> None:
    await migrate_batch_error()


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
