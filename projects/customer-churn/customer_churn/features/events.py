import logging
from datetime import datetime, timedelta

from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)

DEFAULT_START_STATUS = "active"

STATUS_CHANGE = ["changeStatus_Freezed", "changeStatus_Activated"]

NORMAL_ACTIVITIES = [
    "weekPreviewed",
    "weekBreakfastPreviewed",
    "weekPlanned",
    "valgfriPlanNextWeek",
    "changeMenu",
    "ratedRecipe",
    "ProductAdded",
    "ProductViewed",
]

ACCOUNT_MANAGEMENT = [
    "changePayment",
    "Postcodefound",
    "updatePersonalia",
    "passwordformselected",
    "Emailformcompleted",
    "telephoneformselected",
    "Telephoneauthfailure",
    "Postcodenotfound",
    "changeAddress",
    "Postcodenotfound",
]


LABEL_TEXT_ACTIVE = "active"
LABEL_TEXT_DELETED = "deleted"
LABEL_TEXT_CHURNED = "churned"


def get_events_features_for_date(
    env: str,
    company: Company,
    snapshot_date: datetime,
    events_last_n_weeks: int = 4,
    average_last_n_weeks: int = 10,
) -> DataFrame:
    # Load raw events data
    logger.info("Loading events data for company %s", company.company_id)
    df = read_events_table(env, snapshot_date)

    logger.info(df.select("event_text").distinct().collect())

    # Create date variables
    date_event_last_n_weeks = snapshot_date - timedelta(weeks=events_last_n_weeks)
    date_average_last_n_week = snapshot_date - timedelta(weeks=average_last_n_weeks)
    date_last_week = snapshot_date - timedelta(weeks=1)

    # Add aggregated features
    df_aggregated_account_features = get_aggregated_features_for_event(
        df,
        event_type="account",
        date_event_last_n_weeks=date_event_last_n_weeks,
        date_average_last_n_week=date_average_last_n_week,
        date_last_week=date_last_week,
        average_last_n_weeks=average_last_n_weeks,
    )
    df_aggregated_account_features.show(1, vertical=True)

    df_aggregated_normal_features = get_aggregated_features_for_event(
        df,
        event_type="normal",
        date_event_last_n_weeks=date_event_last_n_weeks,
        date_average_last_n_week=date_average_last_n_week,
        date_last_week=date_last_week,
        average_last_n_weeks=average_last_n_weeks,
    )

    df_aggregated_normal_features.show(1, vertical=True)

    # Get status changes
    df_status_changes = df.filter("status_change = 1").withColumn(
        "status",
        f.when(
            f.col("event_text").isin(["activated", "ordered"]),
            LABEL_TEXT_ACTIVE,
        )
        .when(f.col("event_text") == "freezed", LABEL_TEXT_CHURNED)
        .otherwise(LABEL_TEXT_DELETED),
    )

    df_status_changes.show(1, vertical=True)

    # Join all features
    df = (
        df_aggregated_account_features.join(
            df_aggregated_normal_features, on="agreement_id"
        )
        .withColumn("agreement_id", f.col("agreement_id").cast("int"))
        .fillna(0)
    )

    return df


def read_events_table(env: str, snapshot_date: datetime) -> DataFrame:
    return (
        spark.read.table(
            f"{env}.mltesting.events"
        )  # TODO: table names should be one pr company
        .select("agreement_id", "event_text", "timestamp")
        .filter(f"timestamp <= '{snapshot_date}'")
        .withColumn("account_event", f.col("event_text").isin(ACCOUNT_MANAGEMENT))
        .withColumn("normal_event", f.col("event_text").isin(NORMAL_ACTIVITIES))
        .withColumn("status_change", f.col("event_text").isin(STATUS_CHANGE))
    )


def get_aggregated_features_for_event(
    df: DataFrame,
    event_type: str,
    date_event_last_n_weeks: datetime,
    date_average_last_n_week: datetime,
    date_last_week: datetime,
    average_last_n_weeks: int,
) -> DataFrame:
    return (
        df.groupBy("agreement_id")
        .agg(
            f.sum(f.col(f"{event_type}_event").cast("long")).alias(
                f"total_{event_type}_activities"
            ),
            f.sum(
                f.when(
                    f.col("timestamp") > date_event_last_n_weeks,
                    f.col(f"{event_type}_event").cast("long"),
                )
            ).alias(f"number_of_{event_type}_activities_last_N_weeks"),
            f.sum(
                f.when(
                    f.col("timestamp") > date_last_week,
                    f.col(f"{event_type}_event").cast("long"),
                )
            ).alias(f"number_of_{event_type}_activities_last_week"),
            f.sum(
                f.when(
                    f.col("timestamp") > date_average_last_n_week,
                    f.col(f"{event_type}_event").cast("long"),
                )
            ).alias(f"average_{event_type}_activity_difference"),
        )
        .withColumn(
            f"average_{event_type}_activity_difference",
            f.col(f"average_{event_type}_activity_difference") / average_last_n_weeks,
        )
    )
