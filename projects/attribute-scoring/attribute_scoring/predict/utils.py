import logging

import pandas as pd
from attribute_scoring.predict.configs import PredictionConfig
from catalog_connector import connection


CONFIG = PredictionConfig()


def update_is_latest_flag(table_name: str, data: pd.DataFrame) -> None:
    """Updates the `is_latest` flag in the specified table by setting it to False.

    The functions sets the is_latest flag to False for all rows in the target table where the
    combinations of company_id and recipe_id (provided in the dataframe) is found.

    Args:
        table_name (str): Name of target table where is_latest flag will be updated.
        data (pd.DataFrame): DataFrame containing company_id and recipe_id columns.
    """

    unique_combinations = data[["company_id", "recipe_id"]].drop_duplicates()

    spark_df = connection.sql("SELECT 1").sparkSession.createDataFrame(
        unique_combinations
    )
    spark_df.createOrReplaceTempView("update_combinations")

    try:
        connection.sql(f"""
            UPDATE {table_name} AS target
            SET is_latest = False
            WHERE EXISTS (
                SELECT 1
                FROM update_combinations AS source
                WHERE target.company_id = source.company_id
                    AND target.recipe_id = source.recipe_id
            )
        """)
    except Exception as e:
        logging.warning(f"Failed to update is_latest flag: {e}")
    finally:
        try:
            connection.sql("DROP VIEW IF EXISTS update_combinations")
        except Exception as e:
            logging.warning(f"Failed to drop temporary view: {e}")


def save_outputs_to_databricks(
    data: pd.DataFrame, table_name: str | None = None, table_schema: str | None = None
) -> None:
    if table_name is None:
        table_name = "attribute_scoring"
    if table_schema is None:
        table_schema = "mloutputs"

    full_table_name = f"{table_schema}.{table_name}"
    print(f"Writing into {full_table_name}...")

    try:
        spark_df = connection.sql("SELECT 1").sparkSession.createDataFrame(data)
        connection.table(full_table_name).append(spark_df)
    except Exception as e:
        logging.warning(f"Failed to save outputs to Databricks: {e}")
