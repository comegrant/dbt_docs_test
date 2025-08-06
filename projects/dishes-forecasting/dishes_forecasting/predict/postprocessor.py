import logging
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz
from catalog_connector import connection


def save_predictions(
    df_to_write: pd.DataFrame,
    env: str,
    table_name: str,
    table_schema: Optional[str] = "mloutputs",
) -> None:
    full_table_name = f"{env}.{table_schema}.{table_name}"
    logging.info(f"Writing into {full_table_name}...")
    append_pandas_df_to_catalog(df_to_write, full_table_name)


def postprocess_predictions(
    df_predictions: pd.DataFrame
) -> pd.DataFrame:
    run_id = uuid.uuid4()

    df_predictions["run_id"] = str(run_id)
    timezone = pytz.timezone("cet")
    df_predictions["created_at"] = datetime.now(timezone)

    df_to_save = df_predictions[[
        "run_id",
        "company_id",
        "menu_year",
        "menu_week",
        "product_variation_id",
        "product_variation_name",
        "recipe_name",
        "variation_ratio_prediction",
        "created_at"
    ]]
    return df_to_save


def append_pandas_df_to_catalog(df: pd.DataFrame, table_name: str) -> None:

    spark_df = connection.spark().createDataFrame(df)
    connection.table(f"{table_name}").append(spark_df)
