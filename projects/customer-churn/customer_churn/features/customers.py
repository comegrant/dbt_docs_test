import logging
from datetime import datetime

from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def get_customers_features_for_date(
    env: str, company: Company, snapshot_date: datetime
) -> DataFrame:
    df = get_customers_data(env, company, snapshot_date)
    df = (
        df.withColumn(
            "customer_since_weeks",
            f.floor(
                f.datediff(f.lit(snapshot_date), f.col("agreement_start_date")) / 7
            ),
        )
        .withColumn("company_id", f.lit(company.company_id))
        .withColumn("snapshot_date", f.lit(snapshot_date))
    )

    return df


def get_customers_data(
    env: str, company: Company, snapshot_date: datetime
) -> DataFrame:
    df = (
        spark.read.table(f"{env}.mltesting.mb_customers")
        .select(
            "agreement_id",
            "agreement_status",
            "agreement_start_date",
            "agreement_start_year",
            "agreement_start_week",
            "agreement_first_delivery_year",
            "agreement_first_delivery_week",
            "agreement_creation_date",
            "agreement_first_delivery_date",
            "last_delivery_date",
            "next_estimated_delivery_date",
        )
        .filter(f"company_id = '{company.company_id}'")
        .filter("agreement_status_id != 40")
        .filter(f"agreement_start_date <= '{snapshot_date}'")
        .na.fill(
            {"agreement_first_delivery_year": 0, "agreement_first_delivery_week": 0}
        )
    )

    logger.info("Customers data loaded with rows: " + str(df.count()))

    return df
