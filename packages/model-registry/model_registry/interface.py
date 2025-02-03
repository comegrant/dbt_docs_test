from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Union, overload

from mlflow.models import ModelSignature

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyspark.sql as ps
    from aligned.feature_store import ModelFeatureStore
    from aligned.schemas.feature import FeatureReference
    from databricks.feature_store.training_set import TrainingSet
    from pyspark.sql import SparkSession

    DatasetTypes = Union[pd.DataFrame, pl.DataFrame, ps.DataFrame]
    ModelOutputTypes = Union[pd.Series, pl.Series, ps.Column]



class ModelMetadata:
    feature_reference = "feature_refs"
    training_run_url = "training_run_url"
    training_set_reference = "training_set_reference"


def training_run_url(spark: SparkSession) -> str | None:
    job_id = spark.sparkContext.getLocalProperty("spark.databricks.job.id")
    parent_run_id = spark.sparkContext.getLocalProperty("spark.databricks.job.parentRunId")
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

    if job_id and parent_run_id and workspace_url:
        return f"{workspace_url}/jobs/{job_id}/runs/{parent_run_id}"
    else:
        return None


class Model(Protocol):

    @overload
    def predict(self, data: pd.DataFrame) -> pd.Series:
        ...

    @overload
    def predict(self, data: pl.DataFrame) -> pl.Series:
        ...

    @overload
    def predict(self, data: ps.DataFrame) -> ps.Column:
        ...

    def predict(self, data: DatasetTypes) -> ModelOutputTypes:
        ...

    def fit(self, X: DatasetTypes, y: ModelOutputTypes) -> None:  # noqa: N803
        ...

class ModelRegistryBuilder:

    def training_dataset(self, dataset: DatasetTypes | TrainingSet) -> ModelRegistryBuilder:
        return self

    def feature_references(
        self, references: list[str] | list[FeatureReference] | ModelFeatureStore
    ) -> ModelRegistryBuilder:
        return self

    def signature(self, signature: ModelSignature | dict | ModelFeatureStore) -> ModelRegistryBuilder:
        return self

    def training_run_url(self, run_url: str | None) -> ModelRegistryBuilder:
        return self

    async def register_as(self, model_name: str, model: Model) -> None:
        raise NotImplementedError(type(self))
