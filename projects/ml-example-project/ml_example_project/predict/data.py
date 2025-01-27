from datetime import datetime
from typing import Optional

import pytz
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from ml_example_project.db import get_data_from_sql
from ml_example_project.paths import PREDICT_SQL_DIR
from ml_example_project.train.configs import feature_lookup_config_list
from ml_example_project.train.data import get_feature_dataframes


def create_predict_dataframe(
    spark: SparkSession,
    company_id: str,
    predict_start_yyyyww: int,
    predict_end_yyyyww: int,
) -> tuple[DataFrame, DataFrame]:
    """Create dataset to predict on.

    - Gets primary key of data for prediction.
    - Gets feature dataframes and ignore columns, mimicking Databricks FeatureLookup.
    - Merges primary key and feature dataframes.

    Parameters:
        spark (SparkSession): Spark session.
        company_id (str): Company ID.
        predict_start_yyyyww (int): Start year and week.
        predict_end_yyyyww (int): End year and week.

    Returns:
        tuple[DataFrame, DataFrame]: A dataframe with primary key and the feature dataframe for predictions.
    """
    df_predict_pk = get_data_from_sql(
        spark=spark,
        sql_path=PREDICT_SQL_DIR / "data_to_predict.sql",
        predict_start_yyyyww=predict_start_yyyyww,
        predict_end_yyyyww=predict_end_yyyyww,
        company_id=company_id,
    )
    df_list, ignore_columns = get_feature_dataframes(spark=spark, feature_lookup_config_list=feature_lookup_config_list)
    df_predict = df_predict_pk
    for df in df_list:
        # The next three lines infer the common column names
        # in the two dataframes to be merged on
        columns_merged = df_predict.columns
        columns_df = df.columns
        common_columns = list(set(columns_merged).intersection(columns_df))
        df_predict = df_predict.join(df, how="inner", on=common_columns)
    predict_data = df_predict.drop(*ignore_columns)
    df_pk = df_predict[ignore_columns]

    return df_pk, predict_data


def postprocess_predictions(
    spark: SparkSession,
    predicted_data: DataFrame,
    is_run_on_databricks: bool = True,
    df_predict_pk: Optional[DataFrame] = None,
) -> DataFrame:
    """Postprocess predictions.

    Parameters:
        spark (SparkSession): Spark session.
        predicted_data (DataFrame): Predicted data.
        is_run_on_databricks (bool): Whether running on Databricks.
        df_predict_pk (Optional[DataFrame]): Primary key of data for prediction.

    Returns:
        DataFrame: Postprocessed predictions.
    """
    if is_run_on_databricks:
        results = predicted_data.na.drop()
        results = results["recipe_id", "prediction"]
        results = results.withColumnRenamed("prediction", "recipe_difficulty_level_id_prediction")
        results = results.withColumn(
            "recipe_difficulty_level_id_prediction", results["recipe_difficulty_level_id_prediction"].cast("integer")
        )
        results = results.withColumn("predicted_at", current_timestamp())

    else:
        if df_predict_pk is None:
            raise ValueError

        results = df_predict_pk.toPandas()
        results["recipe_difficulty_level_id_prediction"] = predicted_data
        results["predicted_at"] = datetime.now(tz=pytz.timezone("UTC"))
        results = spark.createDataFrame(results)

    return results
