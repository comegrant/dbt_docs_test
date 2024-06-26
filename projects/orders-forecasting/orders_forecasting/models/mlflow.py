from orders_forecasting.paths import CONFIG_DIR
from orders_forecasting.utils import read_yaml
from pydantic import BaseModel


class MlflowConfig(BaseModel):
    experiment_id: int
    experiment_tracking_dir: str
    experiment_name: str
    artifact_path: str
    model_container_name: str
    registered_model_name: str
    mlflow_tracking_uri: str


def get_mlflow_config(env: str) -> MlflowConfig:
    mlflow_config = read_yaml(file_name="mlflow", directory=CONFIG_DIR)
    return MlflowConfig(**mlflow_config[env])
