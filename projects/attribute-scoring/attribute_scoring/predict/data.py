import datetime as dt

import pytz
from attribute_scoring.db import get_data_from_sql
from attribute_scoring.paths import PREDICT_SQL_DIR
from attribute_scoring.predict.configs import PredictionConfig
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame

CONFIG = PredictionConfig()


def get_prediction_data(
    spark: DatabricksSession, company_id: str, start_yyyyww: int | None = None, end_yyyyww: int | None = None
) -> tuple[DataFrame, int, int]:
    """Fetches prediction data from a Databricks SQL table.

    Args:
        spark (DatabricksSession): The Spark session to use for querying.
        company_id (str): The ID of the company to filter data for.
        start_yyyyww (int | None): The start week of the prediction period.
        end_yyyyww (int | None): The end week of the prediction period.

    Returns:
        DataFrame: A DataFrame containing the recipe_id and target label.
    """
    if start_yyyyww is None:
        prediction_date = dt.datetime.now(pytz.timezone("cet")).replace(tzinfo=None) + dt.timedelta(
            weeks=CONFIG.weeks_in_future
        )
        start_yyyyww = int(f"{prediction_date.year}{prediction_date.isocalendar()[1]:02d}")
    if end_yyyyww is None:
        end_yyyyww = start_yyyyww

    df = get_data_from_sql(
        spark=spark,
        sql_path=PREDICT_SQL_DIR / "data_to_predict.sql",
        input_schema=CONFIG.input_schema,
        input_table=CONFIG.input_table,
        company_id=company_id,
        start_yyyyww=start_yyyyww,
        end_yyyyww=end_yyyyww,
    )
    return df, start_yyyyww, end_yyyyww
