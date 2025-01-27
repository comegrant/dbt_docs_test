from typing import Literal

import mlflow
from constants.companies import get_company_by_code
from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from ml_example_project.db import save_outputs
from ml_example_project.predict.configs import get_company_predict_configs
from ml_example_project.predict.data import create_predict_dataframe, postprocess_predictions


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    predict_start_yyyyww: int
    predict_end_yyyyww: int
    env: Literal["dev", "test", "prod"]
    is_run_on_databricks: bool = True
    profile_name: str = "sylvia-liu"


def make_predictions(args: Args, spark: SparkSession) -> DataFrame:
    """Create predictions for specific company and time period.

    - Gets company specific configurations and loads trained model.
    - Creates dataframe to predict on.
    - Generates predictions with timestamp.
    - Saves predictions to databricks table.

    Parameters:
        args (Args): Configuration arguments.
        spark (SparkSession): Spark session.

    Returns:
        DataFrame: Spark dataframe with timestamped predictions.

    """
    company_predict_configs = get_company_predict_configs(company_code=args.company, env=args.env)
    company_properties = get_company_by_code(company_code=args.company)

    if args.is_run_on_databricks:
        mlflow.set_tracking_uri("databricks")
    else:
        mlflow.set_tracking_uri(f"databricks://{args.profile_name}")

    model_uri = company_predict_configs.model_uri

    company_id = company_properties.company_id
    df_predict_pk, predict_data = create_predict_dataframe(
        spark=spark,
        company_id=company_id,
        predict_start_yyyyww=args.predict_start_yyyyww,
        predict_end_yyyyww=args.predict_start_yyyyww,
    )
    if args.is_run_on_databricks:
        fe = FeatureEngineeringClient()
        y_pred = fe.score_batch(model_uri=model_uri, df=df_predict_pk)
        spark_df_result = postprocess_predictions(
            spark=spark, predicted_data=y_pred, is_run_on_databricks=args.is_run_on_databricks
        )
    else:
        loaded_model = mlflow.pyfunc.load_model(model_uri)
        y_pred = loaded_model.predict(predict_data.toPandas())
        spark_df_result = postprocess_predictions(
            spark=spark,
            predicted_data=y_pred,
            is_run_on_databricks=args.is_run_on_databricks,
            df_predict_pk=df_predict_pk,
        )

    save_outputs(spark_df=spark_df_result, table_name="ml_example_project_predictions", table_schema="mloutputs")

    return spark_df_result
