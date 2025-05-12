import logging
import pandas as pd

from catalog_connector import connection


logger = logging.getLogger(__name__)


async def training_dataset(table: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Loads the needed data for the workflow.
    """
    sdf = connection.table(table).read()
    pandas_df = sdf.limit(10).toPandas()

    logger.info(pandas_df)

    return (
        pandas_df,
        [f"{table}.{col}" for col in pandas_df.columns.tolist()]
    )
