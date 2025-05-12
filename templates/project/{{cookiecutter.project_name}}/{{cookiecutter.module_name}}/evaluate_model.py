import mlflow

import pandas as pd

from model_registry import Model


def evaluate_model(model: Model, data: pd.DataFrame) -> None:
    "Evaluate the model with whatever metric is needed"

    mlflow.log_metric("some-metric-of-interest", data.shape[0])
