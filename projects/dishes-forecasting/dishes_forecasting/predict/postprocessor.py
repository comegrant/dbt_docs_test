import logging
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz
from pyspark.sql import SparkSession


def save_predictions(
    df_to_write: pd.DataFrame,
    spark: SparkSession,
    env: str,
    table_name: str,
    table_schema: Optional[str] = "mloutputs",
) -> None:
    full_table_name = f"{env}.{table_schema}.{table_name}"
    spark_df = spark.createDataFrame(df_to_write)
    logging.info(f"Writing into {full_table_name}...")
    spark_df.write.mode("append").saveAsTable(full_table_name)


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
        "portion_size",
        "variation_ratio_prediction",
        "created_at"
    ]]
    return df_to_save
