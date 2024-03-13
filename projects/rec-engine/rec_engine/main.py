import asyncio
import logging
import tracemalloc
from datetime import date, datetime, timedelta, timezone

from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args

from rec_engine.data.store import recommendation_feature_contracts
from rec_engine.logger import Logger
from rec_engine.run import (
    CompanyDataset,
    ManualDataset,
    RateMenuRecipes,
    run,
)

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    company_id: str = Field("6A2D0B60-84D6-4830-9945-58D518D27AC2")

    log_file_dir: str | None = Field(None)
    cache_location: str | None = Field(None)

    year_weeks: list[int] = Field([date.today().strftime("%Y%W")])
    only_for_agreement_ids: list[int] | None = Field(None)

    update_source_threshold: timedelta | None = Field(None)
    ratings_update_threshold: timedelta | None = Field(None)

    train_year_weeks: list[int] | None = Field(None)
    write_to: str = Field(f"data/rec_engine/{date.today().isoformat()}")

    manual_year_week: list[int] | None = Field(None)
    manual_recipe_ids: list[int] | None = Field(None)
    manual_product_ids: list[str] | None = Field(None)
    manual_train_recipe_ids: list[int] | None = Field(None)


def setup_logger(log_dir: str | None) -> None:
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
        logging.ERROR,
    )
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H:%M:%S")
    if log_dir:
        logging.basicConfig(level=logging.INFO, filename=f"{log_dir}/{now}")
    else:
        logging.basicConfig(level=logging.INFO)


async def run_with_args(args: RunArgs, logger: Logger | None = None) -> None:
    if args.manual_year_week and args.manual_recipe_ids and args.manual_product_ids:
        recipe_ids = args.manual_recipe_ids
        year_weeks = args.manual_year_week
        product_ids = args.manual_product_ids

        train_on_recipe_ids = args.manual_train_recipe_ids or recipe_ids

        dataset = ManualDataset(
            train_on_recipe_ids=train_on_recipe_ids,
            rate_menus=RateMenuRecipes(
                year_weeks=year_weeks,
                recipe_ids=recipe_ids,
                product_id=product_ids,
            ),
        )
    elif args.company_id and args.year_weeks:
        year_weeks = args.year_weeks

        if args.train_year_weeks:
            train_year_weeks = args.train_year_weeks
        else:
            today = date.today()
            number_of_weeks = 24
            train_year_weeks = [int((today - timedelta(weeks=i)).strftime("%Y%W")) for i in range(number_of_weeks)]

        dataset = CompanyDataset(
            company_id=args.company_id,
            year_weeks_to_predict_on=year_weeks,
            year_weeks_to_train_on=train_year_weeks,
        )
    else:
        raise ValueError("Unable to create dataset")

    # Reads the repo and finds every feature view and model contract
    store = recommendation_feature_contracts()

    tracemalloc.start()

    setup_logger(args.log_file_dir)

    await run(
        dataset=dataset,
        store=store,
        write_to_path=args.write_to,
        agreement_ids_subset=args.only_for_agreement_ids,
        update_source_threshold=args.update_source_threshold,
        ratings_update_source_threshold=args.ratings_update_threshold,
        logger=logger,
    )


async def cli_run() -> None:
    """
    Trains and predicts using the recommendation engine.

    Can be started with the following command

    ```bash
    python -m rec_engine.main \\
        --manual-year-week 202344 202344 202344 202344 202344 \\
        --manual-recipe-ids 64881 64882 64883 64884 64885 \\
        --manual-product-ids abc bdb yyy zzz uuu \\
        --write-to data/rec_engine
    ```
    """
    args = parser_for(RunArgs).parse_args()

    await run_with_args(
        decode_args(args, RunArgs),
    )


if __name__ == "__main__":
    asyncio.run(cli_run())
