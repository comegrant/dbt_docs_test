import logging
from datetime import datetime, timedelta

from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def get_complaints_features_for_date(
    env: str,
    company: Company,
    snapshot_date: datetime,
    complaints_last_n_weeks: int = 4,
) -> DataFrame:
    df = get_complaints_data(env, company)

    df = df.withColumn(
        "delivery_date",
        f.to_date(
            f.col("delivery_year") * f.lit(1000)
            + f.col("delivery_week") * f.lit(10)
            + f.lit("Mon"),
            "yyyywwEEE",
        ),
    ).filter(f"delivery_date <= '{snapshot_date}'")

    df_agreement_complaints = df.groupby("agreement_id").agg(
        f.count("delivery_date").alias("number_of_complaints"),
        f.count("agreement_id").alias("total_complaints"),
        f.max("delivery_date").alias("last_complaint"),
        f.last("category").alias("category"),
    )

    # Find number of complaints last n weeks
    df_agreement_complaints_last_n_weeks = (
        df.filter(
            f.col("delivery_date")
            >= f.lit(snapshot_date + timedelta(weeks=-complaints_last_n_weeks))
        )
        .groupby("agreement_id")
        .agg(
            f.count("delivery_date").alias("number_of_complaints_last_N_weeks"),
        )
    )

    # Join together number of complaints last n weeks with the other features
    df_agreement_complaints = df_agreement_complaints.join(
        df_agreement_complaints_last_n_weeks,
        on="agreement_id",
        how="left",
    )

    return df_agreement_complaints


def get_complaints_data(env: str, company: Company) -> DataFrame:
    return (
        spark.read.table(f"{env}.mltesting.mb_complaints")
        .filter(f"company_id = '{company.company_id}'")
        .filter("delivery_year >= 2021")
        .select(
            "agreement_id",
            "delivery_year",
            "delivery_week",
            "category",
            "registration_date",
        )
    )
