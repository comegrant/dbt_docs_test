import argparse
import asyncio
import logging
import tracemalloc
from datetime import date, datetime, timedelta

from cheffelo_personalization.rec_engine.data.store import recommendation_feature_contracts
from cheffelo_personalization.rec_engine.run import (
    CompanyDataset,
    ManualDataset,
    RateMenuRecipes,
    run,
)

logger = logging.getLogger(__name__)


def parse_args():
    current_date = date.today()

    parser = argparse.ArgumentParser(description="Run project")
    parser.add_argument(
        "-c",
        "--company-id",
        action="store",
        help="Which company to run for",
        type=str,
        required=False,
        default="6A2D0B60-84D6-4830-9945-58D518D27AC2",
    )
    parser.add_argument(
        "--log-file-dir",
        help="Which directory to store the logs",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--cache-location",
        help="Where to store the cached features for optimized run time",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-y",
        "--year-weeks",
        nargs="*",
        help="The year weeks to run for",
        type=list,
        default=[current_date.strftime("%Y%W")],
    )
    parser.add_argument(
        "--only-for-agreement-ids",
        nargs="*",
        help="The agreement ids to run for",
        type=list,
    )
    parser.add_argument(
        "-t",
        "--train-year-weeks",
        nargs="*",
        help="The year weeks to train for",
        type=list,
    )
    parser.add_argument(
        "-w",
        "--write-to",
        default=f"data/rec_engine/{current_date.isoformat()}",
        help="Where to write the predictions to, default will be data lake",
        required=False,
        type=str,
    )
    parser.add_argument(
        "--manual-year-week",
        nargs="*",
        help="The year weeks to manually predict for. Also need to spesify the --manual-recipe-ids",
        type=list,
    )
    parser.add_argument(
        "--manual-recipe-ids",
        nargs="*",
        help="The recipes to manually predict for. Also need to spesify the --manual-year-week",
        type=list,
    )
    parser.add_argument(
        "--manual-product-ids",
        nargs="*",
        help="The product to manually predict for. Also need to spesify the --manual-year-week",
        type=list,
    )
    parser.add_argument(
        "--manual-train-recipe-ids",
        nargs="*",
        help="The recipe ids to train the model on",
        type=list,
    )

    args = parser.parse_args()
    return args


def setup_logger(log_dir: str | None):
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
        logging.ERROR
    )
    now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    if log_dir:
        logging.basicConfig(level=logging.INFO, filename=f"{log_dir}/{now}")
    else:
        logging.basicConfig(level=logging.INFO)


async def cli_run():
    """
    Trains and predicts using the recommendation engine.

    Can be started with the following command

    ```bash
    python -m cheffelo_personalization.rec_engine.main \\
        --manual-year-week 202344 202344 202344 202344 202344 \\
        --manual-recipe-ids 64881 64882 64883 64884 64885 \\
        --manual-product-ids abc bdb yyy zzz uuu \\
        --write-to data/rec_engine
    ```
    """
    args = parse_args()

    agreement_id_subset = None
    if args.only_for_agreement_ids:
        agreement_id_subset = [int("".join(agreement_id)) for agreement_id in args.only_for_agreement_ids]

    if args.manual_year_week and args.manual_recipe_ids:
        recipe_ids = [int("".join(recipe_id)) for recipe_id in args.manual_recipe_ids]
        year_weeks = [int("".join(yw)) for yw in args.manual_year_week]
        product_ids = ["".join(c) for c in args.manual_product_ids]

        dataset = ManualDataset(
            train_on_recipe_ids=args.manual_train_recipe_ids or recipe_ids,
            rate_menus=RateMenuRecipes(
                year_weeks=year_weeks, recipe_ids=recipe_ids, product_id=product_ids
            ),
        )
    elif args.company_id and args.year_weeks:
        year_weeks = [int("".join(yw)) for yw in args.year_weeks]

        if args.train_year_weeks:
            train_year_weeks = [int("".join(yw)) for yw in args.train_year_weeks]
        else:
            today = date.today()
            number_of_weeks = 24
            train_year_weeks = [
                int((today - timedelta(weeks=i)).strftime("%Y%W"))
                for i in range(number_of_weeks)
            ]

        dataset = CompanyDataset(
            company_id=args.company_id,
            year_weeks_to_predict_on=year_weeks,
            year_weeks_to_train_on=train_year_weeks,
        )
    else:
        raise ValueError("Unable to create dataset")

    # cache_location = (
    #     args.cache_location or f"feature-cache-{datetime.utcnow().timestamp()}.parquet"
    # )

    # Reads the repo and finds every feature view and model contract
    store = recommendation_feature_contracts()

    tracemalloc.start()

    setup_logger(args.log_file_dir)

    await run(
        dataset=dataset,
        store=store,
        write_to_path=args.write_to,
        agreement_ids_subset=agreement_id_subset,
    )


if __name__ == "__main__":
    asyncio.run(cli_run())
