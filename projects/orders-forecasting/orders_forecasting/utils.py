import logging
from pathlib import Path

import pandas as pd
import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def read_yaml(
    file_name: str,
    directory: str | None = None,
) -> dict:
    """Read a yaml file and return the content.

    Args:
        file_name (str): the file name without extension
        directory (str, optional): yaml file directory. Defaults to None.

    Returns:
        Dict: yaml content as a dictionary
    """
    yaml_path = f"{directory}/{file_name}.yml"

    with Path(yaml_path).open() as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


def fetch_data_from_sql(
    sql_name: str,
    directory: Path,
    catalog_name: str,
    catalog_scheme: str,
    **kwargs: dict | None,
) -> pd.DataFrame:
    """Fetch data from sql

    Args:
        read_db (DB): db connection
        sql_name (str): sql name, without extension
        directory (Optional[Path], optional): directory of the sql file.
        Defaults to SQL_DIR.

    Returns:
        pd.DataFrame
    """

    logger.info(f"Get data from {sql_name}...")
    with Path(directory / f"{sql_name}.sql").open() as f:
        query = f.read().format(
            catalog_name=catalog_name, catalog_scheme=catalog_scheme, **kwargs
        )

    df = spark.sql(query).toPandas()
    logger.info(f"Get data from sql successfully with {len(df)} rows.")

    return df
