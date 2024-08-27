from pydantic import BaseModel

from customer_churn.paths import CONFIG_DIR
from customer_churn.utils import read_yaml


class MlflowConfig(BaseModel):
    experiment_tracking_dir: str
    experiment_name: str
    artifact_path: str
    mlmodel_container_name: str
    registered_model_name: str
    mlflow_tracking_uri: str
    experiment_id: int


def get_mlflow_config(env: str) -> MlflowConfig:
    mlflow_config = read_yaml("mlflow", CONFIG_DIR)
    return MlflowConfig(**mlflow_config[env])
