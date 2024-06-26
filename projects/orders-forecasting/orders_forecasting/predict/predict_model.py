import logging

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def make_predictions(model_uri: str, df: DataFrame) -> DataFrame:
    logger.info(f"Using model: {model_uri}")
    logger.info(f"Dataframe - {df.toPandas().head()}")
    try:
        fs = FeatureEngineeringClient()
        scores_df = fs.score_batch(model_uri=model_uri, df=df)
    except Exception as error:
        logger.error(error)

    return scores_df
