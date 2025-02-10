import logging

import pandas as pd
from attribute_scoring.db import get_data_from_sql
from attribute_scoring.paths import PREDICT_SQL_DIR
from attribute_scoring.predict.configs import PredictionConfig
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame

CONFIG = PredictionConfig()


def save_outputs_to_databricks(
    spark_df: DataFrame,
    table_name: str | None = None,
    table_schema: str | None = None,
) -> None:
    """Save (append) data in databricks table.

    Parameters:
        spark_df (DataFrame): Spark dataframe to save.
        table_name (str): Name of the table to write to in Databricks.
        table_schema (str): Name of schema to write to in Databricks.
    """

    if table_name is None:
        table_name = CONFIG.output_table
    if table_schema is None:
        table_schema = CONFIG.output_schema

    full_table_name = f"{table_schema}.{table_name}"
    logging.info(f"Writing into {full_table_name}...")
    spark_df.write.mode("append").saveAsTable(full_table_name)


def update_is_latest_flag(
    spark: DatabricksSession, table_name: str, data: pd.DataFrame
) -> None:
    """Updates the `is_latest` flag in the specified table by setting it to False.

    The functions sets the is_latest flag to False for all rows in the target table where the
    combinations of company_id and recipe_id (provided in the dataframe) is found.

    Args:
        spark (DatabricksSession): A Spark session.
        table_name (str): Name of target table where is_latest flag will be updated.
        data (pd.Dataframe): DataFrame containing company_id and recipe_id columns.
    """
    spark_df = spark.createDataFrame(  # type: ignore
        data[["company_id", "recipe_id"]].drop_duplicates()
    )
    spark_df.createOrReplaceTempView("update_combinations")
    try:
        spark.sql(  # type: ignore
            f"""
            update {table_name} as target
            set is_latest = False
            where exists (
                select 1
                from update_combinations as source
                where target.company_id = source.company_id
                    and target.recipe_id = source.recipe_id
            )
            """
        )
    except Exception as e:
        logging.warning(f"Failed to update is_latest flag: {e}")

    spark.catalog.dropTempView("update_combinations")  # type: ignore


def postprocess_predictions(df: pd.DataFrame, target: str) -> pd.DataFrame:
    """Postprocesses predictions by adding probability and binary target columns.

    Args:
        df (pd.DataFrame): Dataframe containing training data and raw prediction.
        target (str): Target variable name for which predictions were made.

    Return:
        pd.DataFrame: Postprocessed prediction output with prediction probability and binary target columns.
    """
    df = df.dropna()
    df = pd.DataFrame(df[["recipe_id", "prediction"]])

    external_target_name = CONFIG.target_mapped.get(target)
    threshold = CONFIG.prediction_threshold

    df[f"{external_target_name}_probability"] = df["prediction"].astype(float)
    df[f"is_{external_target_name}"] = df["prediction"].apply(
        lambda x: float(x) > threshold
    )

    df = df.drop(["prediction"], axis=1)

    return df


def log_removed_recipes(
    spark: DatabricksSession, company_id: str, start_yyyyww: int, end_yyyyww: int
) -> None:
    """Log recipes that were excluded from prediction due to missing features.

    The function queries recipes with missing feature information using 'data_missing_features.sql'
    and logs their IDs using the logging module. If no recipes were removed, it logs a message
    indicating that.

    Args:
        spark (DatabricksSession): A Spark session.
        company_id (str): Company ID.
        start_yyyyww (int): Start year and week.
        end_yyyyww (int): End year and week.
    """
    df = get_data_from_sql(
        spark=spark,
        sql_path=PREDICT_SQL_DIR / "data_missing_features.sql",
        input_schema=CONFIG.input_schema,
        input_table=CONFIG.input_table,
        company_id=company_id,
        start_yyyyww=start_yyyyww,
        end_yyyyww=end_yyyyww,
    )

    removed_ids = [row.recipe_id for row in df.collect()]

    if removed_ids:
        logging.info(f"Excluded recipe IDs: {removed_ids}")
    else:
        logging.info("No recipe IDs were excluded from prediction.")
