import datetime as dt
import logging
import uuid

import pandas as pd
import pytz
from attribute_scoring.common import Args
from attribute_scoring.predict.config import PredictionConfig
from constants.companies import get_company_by_code
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame

CONFIG = PredictionConfig()


def save_df_to_db(
    args: Args,
    spark: DatabricksSession,
    df: pd.DataFrame,
    start_yyyyww: int,
    end_yyyyww: int,
    output_schema: str | None = None,
    output_table: str | None = None,
) -> None:
    """Saves a dataframe to a Databricks table.

    Args:
        args (Args): Configuration arguments.
        spark (DatabricksSession): A Spark session.
        df (pd.Dataframe): DataFrame to be saved to the Databricks table.
    """
    if output_schema is None:
        output_schema = CONFIG.output_schema
    if output_table is None:
        output_table = CONFIG.output_table

    table_name = f"{args.env}.{output_schema}.{output_table}"
    company_id = get_company_by_code(args.company).company_id
    created_at = dt.datetime.now(pytz.timezone("cet")).replace(tzinfo=None)
    run_id = str(uuid.uuid4())

    logging.info(f"Updating is_latest flag in {table_name}...")
    update_is_latest_flag(spark=spark, table_name=table_name, data=df)

    df["created_at"] = created_at
    df["run_id"] = run_id
    df["is_latest"] = True

    logging.info(f"Writing new data to {table_name}...")
    spark_df = spark.createDataFrame(df.astype(CONFIG.output_columns))
    spark_df.write.mode("append").saveAsTable(table_name)

    logging.info("Creating metadata...")
    metadata = [(run_id, created_at, company_id, start_yyyyww, end_yyyyww)]
    metadata_df = spark.createDataFrame(
        metadata, ["run_id", "run_timestamp", "company_id", "start_yyyyww", "end_yyyyww"]
    )
    table_name_metadata = table_name + "_metadata"

    logging.info(f"Writing metadata to {table_name_metadata}...")
    metadata_df.write.mode("append").saveAsTable(table_name_metadata)


def update_is_latest_flag(spark: DatabricksSession, table_name: str, data: pd.DataFrame) -> None:
    """Updates the `is_latest` flag in the specified table by setting it to False.

    The functions sets the is_latest flag to False for all rows in the target table where the
    combinations of company_id and recipe_id (provided in the dataframe) is found.

    Args:
        spark (DatabricksSession): A Spark session.
        table_name (str): Name of target table where is_latest flag will be updated.
        data (pd.Dataframe): DataFrame containing company_id and recipe_id columns.
    """
    spark_df = spark.createDataFrame(data[["company_id", "recipe_id"]].drop_duplicates())
    spark_df.createOrReplaceTempView("update_combinations")
    try:
        spark.sql(f"""
            update {table_name} as target
            set is_latest = False
            where exists (
                select 1
                from update_combinations as source
                where target.company_id = source.company_id
                    and target.recipe_id = source.recipe_id
            )
        """)
    except Exception as e:
        logging.warning(f"Failed to update is_latest flag: {e}")

    spark.catalog.dropTempView("update_combinations")


def postprocess_predictions(df: pd.DataFrame, target: str) -> pd.DataFrame:
    """Postprocesses predictions by adding probability and binary target columns.

    Args:
        df (pd.DataFrame): Dataframe containing training data and raw prediction.
        target (str): Target variable name for which predictions were made.

    Return:
        pd.DataFrame: Postprocessed prediction output with prediction probability and binary target columns.
    """
    df = df.dropna()
    df = df[["recipe_id", "prediction"]]

    external_target_name = CONFIG.target_mapped.get(target)
    threshold = CONFIG.prediction_threshold

    df[f"{external_target_name}_probability"] = df["prediction"].astype(float)
    df[f"is_{external_target_name}"] = df["prediction"].apply(lambda x: float(x) > threshold)

    df = df.drop(["prediction"], axis=1)

    return df


def filter_data(env: str, spark: DatabricksSession, company_id: str, data: DataFrame) -> DataFrame:
    """Filter the input data by removing recipe IDs that do not have entries in the feature table.

    Args:
        env (str): The environment (e.g., 'dev', 'prod') to fetch data from.
        company_id (str): The ID of the company to filter data for.
        spark (DatabricksSession): A Spark session.
        data (DataFrame): DataFrame containing the initial dataset with recipe IDs.

    Returns:
        DataFrame: DataFrame with only the recipe IDs that have matching entries in the feature table.
    """
    ft_data = spark.sql(
        f"""
        select
            recipe_id
        from {env}.mlfeatures.ft_ml_recipes
        where company_id = '{company_id}'
        """
    )

    removed_data = data.join(ft_data, data["recipe_id"] == ft_data["recipe_id"], "left_anti")
    removed_ids = [row.recipe_id for row in removed_data.collect()]

    if removed_ids:
        logging.info(f"Removed recipe IDs: {removed_ids}")
    else:
        logging.info("No recipe IDs were removed.")

    processed_data = data.join(ft_data, data["recipe_id"] == ft_data["recipe_id"], "inner").select(data["*"])

    return processed_data
