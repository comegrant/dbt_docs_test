from constants.companies import Company
from orders_forecasting.models.mlflow import MlflowConfig
from orders_forecasting.paths import CONFIG_DIR
from orders_forecasting.utils import read_yaml
from pydantic import BaseModel


class MLModelConfig(BaseModel):
    model_name: str
    model_uri: str


class PredictConfig(BaseModel):
    ml_model_config: MLModelConfig
    mlflow_config: MlflowConfig


def get_predict_config(env: str, target_col: str, company: Company) -> PredictConfig:
    predict_config = read_yaml(file_name="predict", directory=CONFIG_DIR)
    mlflow_config = read_yaml(file_name="mlflow", directory=CONFIG_DIR)
    return PredictConfig(
        ml_model_config=predict_config[env][target_col][company.company_code],
        mlflow_config=mlflow_config[env],
    )
