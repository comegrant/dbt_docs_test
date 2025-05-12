from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Protocol, TypeVar, Union, overload

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyspark.sql as ps
    from aligned.feature_store import ModelFeatureStore
    from aligned.schemas.feature import FeatureReference
    from databricks.feature_store.training_set import TrainingSet
    from mlflow.models import ModelSignature
    from pyspark.sql import SparkSession

    DatasetTypes = Union[pd.DataFrame, pl.DataFrame, ps.DataFrame]
    ModelOutputTypes = Union[pd.Series, pl.Series, ps.Column]



class ModelMetadata:
    feature_reference = "feature_refs"
    training_run_url = "training_run_url"
    training_set_reference = "training_set_reference"


def training_run_url(spark: SparkSession) -> str | None:
    job_id: str | None = None
    parent_run_id: str | None = None
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

    with suppress(Exception):
        job_id = spark.sparkContext.getLocalProperty("spark.databricks.job.id")
        parent_run_id = spark.sparkContext.getLocalProperty("spark.databricks.job.parentRunId")

    if not workspace_url:
        return None

    if not job_id:
        return workspace_url

    if not parent_run_id:
        return f"{workspace_url}/jobs/{job_id}"

    return f"{workspace_url}/jobs/{job_id}/runs/{parent_run_id}"


class Model(Protocol):
    @overload
    def predict(self, data: pd.DataFrame) -> pd.Series: ...

    @overload
    def predict(self, data: pl.DataFrame) -> pl.Series: ...

    @overload
    def predict(self, data: ps.DataFrame) -> ps.Column: ...

    def predict(self, data: DatasetTypes) -> ModelOutputTypes: ...

    def fit(self, X: DatasetTypes, y: ModelOutputTypes) -> None:  # noqa: N803
        ...

T = TypeVar("T")

class ModelRegistryBuilder:

    def alias(self: T, alias: str) -> T:
        return self

    def training_dataset(self: T, dataset: DatasetTypes | TrainingSet) -> T:
        return self

    def feature_references(
        self: T, references: list[str] | list[FeatureReference] | ModelFeatureStore
    ) -> T:
        return self

    def signature(self: T, signature: ModelSignature | dict | ModelFeatureStore) -> T:
        return self

    def training_run_url(self: T, run_url: str | None) -> T:
        return self

    async def register_as(self, model_name: str, model: Model) -> None:
        raise NotImplementedError(type(self))
