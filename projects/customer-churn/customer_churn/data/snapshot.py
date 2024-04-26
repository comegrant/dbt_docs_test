# Holds the dataprep logic.

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from constants.companies import Company
from data_connector.databricks_connector import (
    get_spark_session,
    is_running_databricks,
)

from customer_churn.data.load_data import DataLoader
from customer_churn.paths import DATA_PROCESSED_DIR

logger = logging.getLogger(__name__)


def generate_snapshots_for_period(
    data_loader: DataLoader,
    company: Company,
    start_date: datetime,
    end_date: datetime,
    save_snapshot: bool = False,
    env: str = "dev",
    schema: str = "mltesting",
) -> list:
    snapshot_dates = get_snapshot_dates_based_on_company(
        start_date,
        end_date,
        company,
    )
    local_files = []
    for i, snapshot_date in enumerate(snapshot_dates):
        df_out = generate_snapshot_for_date(
            data_loader,
            snapshot_date,
            company,
        )

        if save_snapshot:
            local_path = save_snapshot_for_date(
                df_out,
                snapshot_date,
                company,
                env,
                schema,
                i,
            )
            if local_path:
                local_files.append(local_path)
            logger.info("Snapshot saved!")

    if len(local_files) > 0:
        merge_snapshots_to_one_file(local_files, company)


def merge_snapshots_to_one_file(local_files: list, company: Company) -> None:
    # Merge all snapshots into one file
    logger.info("Merging snapshots into one file")
    local_files.sort()

    df = pd.concat([pd.read_csv(file) for file in local_files])
    df.to_csv(
        DATA_PROCESSED_DIR / company.company_code / "full_snapshot.csv",
        index=False,
    )


def generate_snapshot_for_date(
    data_loader: DataLoader,
    snapshot_date: datetime,
    company: Company,
) -> None:
    snapshot_dt_start = time.time()

    # Get customers for given snapshot date
    logger.info(
        f"Getting snapshot for: {snapshot_date}",
    )

    # Generate data for the given date
    df_out = data_loader.generate_features_for_date(snapshot_date=snapshot_date)
    df_out["company_id"] = company.company_id

    snapshot_dt_end = time.time()
    logger.info(
        "Snapshot done in " + str(int(snapshot_dt_end - snapshot_dt_start)) + " (sec)",
    )

    return df_out


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


def save_snapshot_for_date(
    df_out: pd.DataFrame,
    snapshot_date: datetime,
    company: Company,
    env: str,
    schema: str,
    iterate: int = 0,
    table_name: str = "customer_churn_snapshot",
) -> None | Path:
    if is_running_databricks():
        # Insert result to table on databricks
        logger.info("Storing snapshot for on databricks")
        res = False
        if iterate == 0:
            res = create_table_if_not_exists(
                df_out,
                env,
                schema,
                table_name,
                ["snapshot_date", "company_id"],
            )
        if not res:
            insert_overwrite_table_for_date(
                df_out,
                table_name,
                env,
                schema,
                company.company_id,
                snapshot_date,
            )
    else:  # local
        # Save locally
        local_path = (
            DATA_PROCESSED_DIR
            / company.company_code
            / f"snapshot_{snapshot_date.date().isoformat()}.csv"
        )
        logger.info(f"Save file to path: {local_path}")
        with Path.open(local_path, "a") as f:
            df_out.to_csv(local_path, mode="a", header=f.tell() == 0, index=False)

        return local_path


def create_table_if_not_exists(
    dataframe: pd.DataFrame,
    env: str,
    schema: str,
    table_name: str,
    partition_columns: list,
) -> bool:
    spark = get_spark_session()
    df = spark.createDataFrame(dataframe)

    logger.info(f"Create a temporary view {table_name}_temp_view")
    df.createOrReplaceTempView(f"{table_name}_temp_view")

    logger.info("Create table if not exists")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {env}.{schema}.{table_name}
        USING DELTA
        PARTITIONED BY ({",".join(partition_columns)})
        AS
        SELECT *
        FROM {table_name}_temp_view
        """,
    )
    return spark.table(f"{env}.{schema}.{table_name}").count() > 0


def insert_overwrite_table_for_date(
    dataframe: pd.DataFrame,
    table_name: str,
    env: str,
    schema: str,
    company_id: str,
    snapshot_date: datetime,
) -> None:
    # Use Spark to write data
    spark = get_spark_session()
    df = spark.createDataFrame(dataframe)

    logger.info(f"Create a temporary view {table_name}_temp_view")
    df.createOrReplaceTempView(f"{table_name}_temp_view")

    logger.info("Append the table from the temporary view")
    spark.sql(
        f"""
        INSERT OVERWRITE TABLE {env}.{schema}.{table_name} PARTITION (
            snapshot_date='{snapshot_date.date()}', company_id='{company_id}'
        )
        SELECT
            agreement_id,
            agreement_start_year,
            agreement_start_week,
            agreement_first_delivery_year,
            agreement_first_delivery_week,
            agreement_status,
            agreement_start_date,
            customer_since_weeks,
            total_normal_activities,
            number_of_normal_activities_last_week,
            average_normal_activity_difference,
            number_of_normal_activities_last_N_weeks,
            total_account_activities,
            number_of_account_activities_last_week,
            average_account_activity_difference,
            number_of_account_activities_last_N_weeks,
            snapshot_status,
            forecast_status,
            weeks_since_last_delivery,
            number_of_total_orders,
            current_delivery_year,
            current_delivery_week,
            planned_delivery,
            delivery_date,
            total_complaints,
            weeks_since_last_complaint,
            number_of_complaints_last_n_weeks,
            category,
            year,
            month,
            week
        FROM {table_name}_temp_view
        WHERE snapshot_date='{snapshot_date.date()}' AND company_id='{company_id}';
        """,
    )
