from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame


def get_data_from_sql(sql_path: Path, **kwargs: Any) -> DataFrame:  # noqa
    """Get data from SQL query file."""
    with sql_path.open() as f:
        custom_query = f.read().format(**kwargs)
    from catalog_connector import connection

    df = connection.sql(custom_query)
    return df
