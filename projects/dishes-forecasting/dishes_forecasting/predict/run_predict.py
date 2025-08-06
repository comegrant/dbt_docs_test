from datetime import date
from typing import Literal

import mlflow
from constants.companies import get_company_by_code
from pydantic import BaseModel
from pyspark.sql import DataFrame
from time_machine.forecasting import get_forecast_start

from dishes_forecasting.predict.configs import get_configs
from dishes_forecasting.predict.data import create_pred_dataset
from dishes_forecasting.predict.postprocessor import postprocess_predictions, save_predictions
from dishes_forecasting.predict.predictor import make_predictions
from dishes_forecasting.train.configs.feature_lookup_config import feature_lookup_config_list


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "test", "prod"]
    forecast_date: str = None
    is_running_on_databricks: bool
    profile_name: str = "sylvia-liu"


def run_predict(args: Args) -> DataFrame:
    company = get_company_by_code(args.company)
    pred_configs = get_configs(company_code=args.company)
    if (args.forecast_date is None) or args.forecast_date == "":
        forecast_date = date.today()
    year, week = get_forecast_start(cut_off_day=company.cut_off_week_day, forecast_date=forecast_date)
    min_yyyyww = year * 100 + week
    df_pred, df_left = create_pred_dataset(
        min_yyyyww=min_yyyyww,
        company=company,
        feature_lookup_config_list=feature_lookup_config_list,
        schema="mlgold",
    )
    model_uri = pred_configs.model_uri[args.env]
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    df_predictions = make_predictions(model=loaded_model, df_pred=df_pred)

    df_processed = postprocess_predictions(df_predictions=df_predictions)
    save_predictions(
        df_to_write=df_processed,
        env=args.env,
        table_schema="mloutputs",
        table_name="ml_dishes_ratios",
    )

    return df_predictions
