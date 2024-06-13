from typing import Literal

from constants.companies import get_company_by_code
from databricks.feature_store import FeatureStoreClient
from pydantic import BaseModel, Field
from pyspark.sql import DataFrame

from dishes_forecasting.paths import CONFIG_DIR
from dishes_forecasting.predict.get_data import download_weekly_variations
from dishes_forecasting.utils import read_yaml


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    prediction_date: str
    num_weeks: int = Field(gt=10, lt=21)


def run_predict(args: Args) -> DataFrame:
    company = get_company_by_code(args.company)
    df = download_weekly_variations(
        company=company,
        prediction_date=args.prediction_date,
        num_weeks=int(args.num_weeks),
        env=args.env,
    )

    predict_configs = read_yaml(directory=CONFIG_DIR, filename="predict.yml")
    predict_config = predict_configs[company.company_code]
    model_uri = predict_config["model_uri"]

    fs = FeatureStoreClient()
    scores_df = fs.score_batch(model_uri, df)
    return scores_df
