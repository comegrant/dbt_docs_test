import datetime as dt

from attribute_scoring.predict.config import PredictionConfig
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame

CONFIG = PredictionConfig()


def get_data(
    env: str, spark: DatabricksSession, company_id: str, start_yyyyww: int | None = None, end_yyyyww: int | None = None
) -> DataFrame:
    if start_yyyyww is None:
        prediction_date = dt.datetime.today() + dt.timedelta(weeks=CONFIG.weeks_in_future)
        start_yyyyww = int(f"{prediction_date.year}{prediction_date.isocalendar()[1]:02d}")
    if end_yyyyww is None:
        end_yyyyww = start_yyyyww
    data = spark.sql(
        f"""
        select distinct
            recipe_id
        from {env}.{CONFIG.input_schema}.{CONFIG.input_table}
        where (menu_year*100 + menu_week) between '{start_yyyyww}' and '{end_yyyyww}'
        and company_id = '{company_id}'
        """
    )

    return data, start_yyyyww, end_yyyyww
