import datetime
import logging

from constants.companies import Company
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as f

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


def predict_customer_churn_for_date(
    env: str,
    company: Company,
    prediction_date: datetime.date,
    model_uri: str,
) -> DataFrame:
    logger.info(
        f"Make predictions with model uri {model_uri} on date {prediction_date} in environment {env}"
    )

    df = get_customer_churn_entities(
        env=env,
        company=company,
        prediction_date=prediction_date,
    )

    predictions = make_predictions(model_uri=model_uri, df=df)

    return predictions


def make_predictions(model_uri: str, df: DataFrame) -> DataFrame:
    logger.info(f"Using model: {model_uri}")
    logger.info(f"Dataframe - {df.toPandas().head()}")
    try:
        fs = FeatureEngineeringClient()
        scores_df = fs.score_batch(model_uri=model_uri, df=df, result_type="string")
    except Exception as error:
        logger.error(error)

    return scores_df


def get_customer_churn_entities(
    env: str, company: Company, prediction_date: datetime.date
) -> DataFrame:
    company_id = company.company_id
    # Get all customers for company that we want to predict churn on, which have updated features
    one_week_ago = prediction_date - datetime.timedelta(days=7)
    tomorrow = prediction_date + datetime.timedelta(days=1)

    window = Window.partitionBy("agreement_id").orderBy(f.desc("snapshot_date"))
    df = (
        spark.read.table(f"{env}.mlfeaturetesting.customer_snapshot_features")
        .filter(f"company_id = '{company_id}'")
        .filter(f"snapshot_date between '{one_week_ago}' and '{tomorrow}'")
        .select("agreement_id", "company_id", "snapshot_date")
        .withColumn("rank", f.row_number().over(window))
        .filter("rank = 1")
        .drop("rank")
    )

    return df
