from datetime import datetime
from typing import Literal

import mlflow
import pytz
from constants.companies import get_company_by_code
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from ml_example_project.db import save_outputs
from ml_example_project.predict.configs import get_company_predict_configs
from ml_example_project.predict.data import create_predict_dataframe


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    predict_start_yyyyww: int
    predict_end_yyyyww: int
    env: Literal["dev", "test", "prod"]
    is_run_on_databricks: bool = True
    profile_name: str = "sylvia-liu"


def make_predictions(args: Args, spark: SparkSession) -> DataFrame:
    company_predict_configs = get_company_predict_configs(company_code=args.company)
    company_properties = get_company_by_code(company_code=args.company)

    if args.is_run_on_databricks:
        mlflow.set_tracking_uri("databricks")
    else:
        mlflow.set_tracking_uri(f"databricks://{args.profile_name}")

    model_uri = company_predict_configs.model_uri
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    df_predict_pk, predict_data = create_predict_dataframe(
        spark=spark,
        company_id=company_properties.company_id,
        predict_start_yyyyww=args.predict_start_yyyyww,
        predict_end_yyyyww=args.predict_start_yyyyww,
    )

    y_pred = loaded_model.predict(predict_data.toPandas())
    df_result = df_predict_pk.toPandas()
    df_result["recipe_difficulty_level_id_prediction"] = y_pred
    timezone = pytz.timezone("UTC")
    timestamp_now = datetime.now(tz=timezone)
    df_result["predicted_at"] = timestamp_now
    spark_df_result = spark.createDataFrame(df_result)

    save_outputs(
        spark_df=spark_df_result,
        table_name="ml_example_project_predictions",
        table_schema="mloutputs"
    )

    return spark_df_result
