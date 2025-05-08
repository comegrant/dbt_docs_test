from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame


def get_data_from_sql(sql_path: Path, **kwargs: Any) -> DataFrame:  # noqa
    """Get data from SQL query file."""
    with sql_path.open() as f:
        custom_query = f.read().format(**kwargs)
    from catalog_connector import connection

    df = connection.sql(custom_query)
    return df


def append_pandas_df_to_catalog(df: pd.DataFrame, table_name: str, env: str) -> None:
    from catalog_connector import connection

    spark_df = connection.spark().createDataFrame(df)
    connection.table(f"{env}.{table_name}").append(spark_df)
