import datetime as dt

import pytz
import pandas as pd

from attribute_scoring.predict.configs import PredictionConfig
from attribute_scoring.train.configs import DataConfig
from catalog_connector import connection
from pathlib import Path
from attribute_scoring.paths import PREDICT_SQL_DIR
from attribute_scoring.predict.configs import validate_prediction_data


CONFIG = PredictionConfig()
DATA_CONFIG = DataConfig()


def get_prediction_data(
    company_id: str, start_yyyyww: str, end_yyyyww: str
) -> tuple[pd.DataFrame, int, int]:
    if not start_yyyyww.strip():
        prediction_date = dt.datetime.now(pytz.timezone("cet")).replace(tzinfo=None)
        prediction_date += dt.timedelta(weeks=CONFIG.weeks_in_future)
        year = prediction_date.year
        week = prediction_date.isocalendar()[1]
        start_week = int(f"{year}{week:02d}")
    else:
        start_week = int(start_yyyyww.strip())

    if not end_yyyyww.strip():
        end_week = start_week
    else:
        end_week = int(end_yyyyww.strip())

    recipe_columns_prefixed = ", ".join(
        [f"recipe_features.{col}" for col in DATA_CONFIG.recipe_features.feature_names]
    )
    ingredient_columns_prefixed = ", ".join(
        [
            f"ingredient_features.{col}"
            for col in DATA_CONFIG.ingredient_features.feature_names
        ]
    )

    sql_path = Path(PREDICT_SQL_DIR) / "data_to_predict.sql"
    with sql_path.open() as f:
        query = f.read().format(
            company_id=company_id,
            start_yyyyww=start_week,
            end_yyyyww=end_week,
            recipe_columns_prefixed=recipe_columns_prefixed,
            ingredient_columns_prefixed=ingredient_columns_prefixed,
        )

    df = connection.sql(query).toPandas()

    df = df.dropna()

    df = validate_prediction_data(df)

    return df, start_week, end_week


def postprocessing(
    predictions: pd.DataFrame, company_id: str, target_name: str
) -> pd.DataFrame:
    df = pd.DataFrame(predictions[["recipe_id", "probability"]])

    df["company_id"] = company_id

    df = df.rename(columns={"probability": f"{target_name}_probability"})

    df[f"is_{target_name}"] = df[f"{target_name}_probability"].apply(
        lambda x: float(x) > CONFIG.prediction_threshold
    )

    return df
