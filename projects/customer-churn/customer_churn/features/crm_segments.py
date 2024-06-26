import logging
from datetime import datetime

from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def get_crm_segments_features_for_date(
    env: str, company: Company, snapshot_date: datetime, backfilling: bool = False
) -> DataFrame:
    df = (
        get_crm_segments_data_from_log(env, company)
        if backfilling
        else get_crm_segments_data_latest(env, company)
    )

    df = df.withColumn(
        "delivery_date",
        f.to_date(
            f.col("current_delivery_year") * f.lit(1000)
            + f.col("current_delivery_week") * f.lit(10)
            + f.lit("Mon"),
            "yyyywwEEE",
        ),
    ).filter(f"delivery_date <= '{snapshot_date}'")

    df = (
        df.sort("delivery_date", ascending=False)
        .groupby("agreement_id")
        .agg(
            f.first("planned_delivery").alias("planned_delivery"),
            f.first("sub_segment_name").alias("sub_segment_name"),
            f.first("bargain_hunter").alias("bargain_hunter"),
        )
    )

    return df


def get_crm_segments_data_latest(env: str, company: Company) -> DataFrame:
    return (
        spark.read.table(f"{env}.mltesting.analytics_crm_segment_agreement_main_latest")
        .filter(f"company_id = '{company.company_id}'")
        .filter("current_delivery_year >= 2019")
        .filter("main_segment_name = 'Buyer'")
        .select(
            "agreement_id",
            "current_delivery_year",
            "current_delivery_week",
            "planned_delivery",
            "sub_segment_name",
            "bargain_hunter",
        )
    )


def get_crm_segments_data_from_log(env: str, company: Company) -> DataFrame:
    return (
        spark.read.table(f"{env}.mltesting.analytics_crm_segment_agreement_main_log")
        .filter(f"company_id = '{company.company_id}'")
        .filter("current_delivery_year >= 2019")
        .filter("sub_segment_name != 'Deleted'")
        .select(
            "agreement_id",
            "current_delivery_year",
            "current_delivery_week",
            "planned_delivery",
            "sub_segment_name",
            "bargain_hunter",
        )
    )
