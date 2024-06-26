from constants.companies import Company
from orders_forecasting.paths import CONFIG_DIR
from orders_forecasting.utils import read_yaml
from pydantic import BaseModel


class TrainingFeaturesConfig(BaseModel):
    categorical_features: list
    order_estimation_history_retention_features: list
    order_estimation_features: list
    holidays_features: list

    @property
    def numeric_features(self) -> list:
        return (
            self.order_estimation_history_retention_features
            + self.order_estimation_features
            + self.holidays_features
        )


class MLTrainConfig(BaseModel):
    num_holdout_weeks: int
    fold_strategy: str
    data_split_shuffle: bool


class TrainConfig(BaseModel):
    train_features: TrainingFeaturesConfig
    n_select: int
    model_list: list
    ml_config: MLTrainConfig


def get_train_config(env: str, target_col: str, company: Company) -> TrainConfig:
    train_config = read_yaml(file_name="train", directory=CONFIG_DIR)

    train_config = train_config[env]
    train_features_config = train_config["train_features"]

    train_features_config = TrainingFeaturesConfig(
        order_estimation_history_retention_features=train_features_config[
            "order_estimation_history_retention_features"
        ][target_col],
        order_estimation_features=train_features_config["order_estimation_features"],
        holidays_features=train_features_config[
            f"holidays_features_{company.country.lower()}"
        ],
        categorical_features=train_features_config["categorical_features"],
    )

    return TrainConfig(
        train_features=train_features_config,
        n_select=train_config["n_select"],
        model_list=train_config["model_list"],
        ml_config=MLTrainConfig(**train_config["model_config"]),
    )
