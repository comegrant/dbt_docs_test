import argparse
import logging
from datetime import datetime, timedelta

import pandas as pd
from lmkgroup_ds_utils.azure.storage import BlobConnector
from lmkgroup_ds_utils.db.connector import DB

from preselector.data.models.utils import Yearweek
from preselector.data.retriever import get_data_from_db
from preselector.tests.utils import load_test_data
from preselector.utils.storage import (
    upload_outputs_to_datalake,
    write_result_to_file,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_default_yearweek():
    now = datetime.now()
    two_weeks_in_the_future = now + timedelta(days=14)
    two_weeks_in_the_future = two_weeks_in_the_future.isocalendar()

    return two_weeks_in_the_future.year, two_weeks_in_the_future.week


def get_args():
    default_year, default_week = get_default_yearweek()

    parser = argparse.ArgumentParser(description="Run project")
    parser.add_argument(
        "-c",
        "--company",
        default="gl",
        action="store",
        help="Which company to run for",
        type=str,
        required=False,
    )

    parser.add_argument(
        "-w",
        "--week",
        default=default_week,
        action="store",
        help="What week to preselect dishes for",
        type=int,
        required=False,
    )

    parser.add_argument(
        "-y",
        "--year",
        default=default_year,
        action="store",
        help="What week to preselect dishes for",
        type=int,
        required=False,
    )

    parser.add_argument("--save-output-locally", action="store_true")
    parser.add_argument("--write-to-datalake", action="store_true")
    parser.add_argument("--test", action="store_true")

    parser.add_argument(
        "--config",
        default="default",
        action="store",
        help="Which config to run pipeline for",
        type=str,
        required=False,
    )
    parser.add_argument("--local", action="store", default=True, type=bool)
    parser.add_argument(
        "--dl-tenant-id",
        action="store",
        help="datalake tenant ID",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dl-client-id",
        action="store",
        help="datalake client id",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dl-client-secret",
        action="store",
        help="datalake client secret",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dl-account-url",
        action="store",
        help="datalake account url",
        type=str,
        required=False,
    )
    args = parser.parse_args()

    return args


def run_test():
    (
        agreements,
        product_information,
        menu_products,
        recommendation_scores,
        preference_rules,
    ) = load_test_data()
    run_preselector_batch_api(
        agreements,
        product_information,
        menu_products,
        recommendation_scores,
        preference_rules,
    )


def load_data_from_adb(
    local: bool,
    company: str,
    year: int,
    week: int,
    config: str,
    run_config: dict,
):
    yearweek = Yearweek(year=year, week=week)
    config_dict = get_company_configs(company_code=company, config_name=config)
    company_id = config_dict["company_id"]

    logger.info("Connecting to db...")
    read_db = DB(local=local, db_name="analytics_db")

    logger.info(f"Downloading data from db for {company}...")
    (
        df_customers,
        df_menu,
        df_recommendations,
        df_preference_rules,
        df_quarantined_dishes,
    ) = get_data_from_db(company_id, yearweek, read_db, run_config)
    return (
        df_customers,
        df_menu,
        df_recommendations,
        df_preference_rules,
        df_quarantined_dishes,
    )


def run(
    local: bool,
    company: str = "GL",
    year: int = 2023,
    week: int = 10,
    save_output_locally: bool = False,
    write_to_datalake: bool = False,
    config: str = "default",
    dl_tenant_id: str | None = None,
    dl_client_id: str | None = None,
    dl_client_secret: str | None = None,
    dl_account_url: str | None = None,
):
    """The run function to actually run the main code.

    Args:
        company (str, optional): company code to run order forecasting for.
            Default to "GL"
        config (str, optional): which config to run. Defaults to "default"
        num_weeks (int, optional): how long to run forecast for. Default to 10.
    """
    run_timestamp = datetime.now()
    run_config = get_run_configs()
    (
        df_customers,
        df_menu,
        df_recommendations,
        df_preference_rules,
        df_quarantined_dishes,
    ) = load_data_from_adb(
        local=local,
        company=company,
        year=year,
        week=week,
        config=config,
        run_config=run_config,
    )
    logger.info("Data downloaded.")

    logger.info("Run preselector in batch")
    output = run_preselector_batch_locally(
        df_customers,
        df_menu,
        df_recommendations,
        df_preference_rules,
        df_quarantined_dishes,
        run_config=run_config,
    )

    if save_output_locally or write_to_datalake:
        logger.info(
            "--save-output-locally (or --write-to-datalake) flag set, saving outputs locally",
        )

        write_result_to_file(output, fn="customers_preselected_dishes.json")

    if write_to_datalake:
        logger.info("--write-to-datalake flag set, writing to datalake...")
        datalake_handler = BlobConnector(
            local=local,
            tenant_id=dl_tenant_id,
            client_id=dl_client_id,
            client_secret=dl_client_secret,
            account_url=dl_account_url,
        )

        upload_outputs_to_datalake(
            company=company,
            datalake_handler=datalake_handler,
            df=pd.DataFrame(output),
            file_name="customers_preselected_dishes.csv",
            subdirectory=run_timestamp.strftime("%Y%m%d%H%M%S"),
        )


def main():
    args = get_args()
    print(f"Running preselector with input {args}")
    if args.test:
        run_test()
    else:
        run(
            company=args.company,
            year=args.year,
            week=args.week,
            local=args.local,
            save_output_locally=args.save_output_locally,
            write_to_datalake=args.write_to_datalake,
            dl_tenant_id=args.dl_tenant_id,
            dl_client_id=args.dl_client_id,
            dl_client_secret=args.dl_client_secret,
            dl_account_url=args.dl_account_url,
        )


if __name__ == "__main__":
    main()
