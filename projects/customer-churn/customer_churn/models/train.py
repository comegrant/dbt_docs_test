from pydantic import BaseModel

from customer_churn.paths import CONFIG_DIR
from customer_churn.utils import read_yaml


class MLModelConfig(BaseModel):
    num_holdout_weeks: int
    target: str
    fold_strategy: str
    data_split_shuffle: bool
    transform_target: bool
    transform_target_method: str
    verbose: bool
    html: bool
    n_select: int
    ml_model_list: list[str]


class TrainConfig(BaseModel):
    target_col: str
    train_start_yyyyww: int
    num_holdout_weeks: int
    buy_history_churn_weeks: int
    ml_model_config: MLModelConfig


def get_train_config() -> TrainConfig:
    train_config = read_yaml("train", CONFIG_DIR)
    return TrainConfig(**train_config)
