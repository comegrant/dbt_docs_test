from uuid import uuid4

import mlflow
import pandas as pd
import pytest
from model_registry import ModelMetadata


@pytest.mark.asyncio
async def test_mlflow() -> None:
    from model_registry.mlflow import mlflow_registry

    alias = "champion"
    run_url = f"https://test.com/{uuid4()}"
    model_name = "test"
    example_input = pd.DataFrame({"a": [1, 2, 3], "b": [2, 3, 1]})

    def function(data: pd.DataFrame) -> pd.Series:
        return data.sum(axis=1, numeric_only=True)

    registry = mlflow_registry()

    await (
        registry.alias(alias)
        .training_run_url(run_url)
        .training_dataset(example_input)
        .register_as(model_name, function)  # type: ignore
    )

    latest_version = mlflow.MlflowClient().get_model_version_by_alias(model_name, alias)
    uri = f"models:/{model_name}/{latest_version.version}"
    model = mlflow.pyfunc.load_model(uri)

    metadata = model.metadata.metadata
    assert ModelMetadata.training_run_url in metadata
    assert metadata[ModelMetadata.training_run_url] == run_url

    preds = model.predict(example_input)
    assert preds.shape[0] == example_input.shape[0]
