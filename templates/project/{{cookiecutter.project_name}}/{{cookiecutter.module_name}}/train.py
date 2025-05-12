import mlflow

import pandas as pd

from model_registry import Model


def train_model(data: pd.DataFrame) -> Model:
    """
    The code that trains the model

    Returns:
        A fully trained model
    """

    def sum_function(data: pd.DataFrame):  # noqa
        return data.sum(axis=1, numeric_only=True)

    return mlflow.pyfunc.model._FunctionPythonModel(sum_function)  # type: ignore
