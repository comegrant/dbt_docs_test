import logging
from datetime import datetime

from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def get_orders_features_for_date(
    env: str,
    company: Company,
    snapshot_date: datetime,
) -> DataFrame:
    df = get_orders_data(env, company)
    df_orders = df.filter(f"delivery_date <= '{snapshot_date}'")

    df_orders_features = (
        df_orders.groupby("agreement_id")
        .agg(
            f.max("delivery_date").alias("max_delivery_date"),
            f.count("delivery_date").alias("number_of_total_orders"),
        )
        .withColumn(
            "weeks_since_last_delivery",
            f.floor(
                (
                    f.date_diff(f.lit(snapshot_date), f.col("max_delivery_date")) / 7
                ).cast("int")
            ),
        )
    )

    return df_orders_features


def get_orders_data(env: str, company: Company) -> DataFrame:
    return (
        spark.read.table(f"{env}.mltesting.mb_orders")
        .filter(f"company_id = '{company.company_id}'")
        .filter("delivery_year >= 2021")
        .select(
            "agreement_id",
            "company_name",
            "order_id",
            "delivery_date",
            "delivery_year",
            "delivery_week",
            "net_revenue_ex_vat",
            "gross_revenue_ex_vat",
        )
        .withColumn("year", f.year("delivery_date"))
        .withColumn("week", f.weekofyear("delivery_date"))
        .withColumn("month", f.month("delivery_date"))
    )
