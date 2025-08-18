from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar, Union, overload

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyspark.sql as ps
    from aligned.feature_store import ModelFeatureStore
    from aligned.schemas.feature import FeatureReference
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


@dataclass
class ModelRef:
    name: str
    version: str
    version_type: Literal["alias", "version"]

    @property
    def model_uri(self) -> str:
        if self.version_type == "alias":
            return f"models:/{self.name}@{self.version}"
        else:
            return f"models:/{self.name}/{self.version}"

    @staticmethod
    def from_string(model: str | ModelRef) -> ModelRef:
        if isinstance(model, ModelRef):
            return model

        model = model.removeprefix("models:/")

        if "/" in model:
            name, alias = model.split("/")
            return ModelRef(name, alias, "version")
        elif "@" in model:
            name, alias = model.split("@")
            return ModelRef(name, alias, "alias")
        else:
            raise ValueError(f"Unable to decode '{model}'")


T = TypeVar("T")


class ModelRegistryBuilder:
    @overload
    def infer_over(
        self,
        entities: pl.DataFrame,
        model_uri: str | ModelRef,
        feature_refs: list[str] | None = None,
        output_name: str | None = None,
    ) -> pl.DataFrame: ...

    @overload
    def infer_over(
        self,
        entities: pd.DataFrame,
        model_uri: str | ModelRef,
        feature_refs: list[str] | None = None,
        output_name: str | None = None,
    ) -> pd.DataFrame: ...

    @overload
    def infer_over(
        self,
        entities: pl.LazyFrame,
        model_uri: str | ModelRef,
        feature_refs: list[str] | None = None,
        output_name: str | None = None,
    ) -> pl.LazyFrame: ...

    def infer_over(
        self,
        entities: pd.DataFrame | pl.DataFrame | pl.LazyFrame,
        model_uri: str | ModelRef,
        feature_refs: list[str] | None = None,
        output_name: str | None = None,
    ) -> pd.DataFrame | pl.DataFrame | pl.LazyFrame:
        """
        Uses the defined model to infer over a set of entities.

        To use it in it's simplest form, do it assumes that both a feature reference and a model signature is set.

        ```python
        entities = pl.DataFrame({"recipe_id": [...]})

        preds = registry.infer_over(entities, "my-awesome-model@champion")
        ```
        Otherwise the feature refs and output name can be manually defined.


        ```python
        preds = registry.infer_over(entities, "my-awesome-model/1", output_name="preds", feature_refs=[
            "mlgold.ml_recipes.cooking_time_from",
            "mlgold.ml_recipes.cooking_time_to",
            "mlgold.ml_recipes.number_of_taxonomies",
            ...
        ])
        ```

        Arguments:
            entities: The rows to infer over
            model_uri: A reference to the model to use
            feature_refs: A manual overwrite of the feature references to load into the model
            output_name: A manual overwrite of what to store the output as

        Returns:
            A dataframe containing both the entities and it's output.
        """
        raise NotImplementedError(type(self))

    def alias(self: T, alias: str) -> T:
        return self

    def training_dataset(self: T, dataset: DatasetTypes) -> T:
        return self

    def feature_references(self: T, references: list[str] | list[FeatureReference] | ModelFeatureStore) -> T:
        return self

    def signature(self: T, signature: ModelSignature | dict | ModelFeatureStore) -> T:
        return self

    def training_run_url(self: T, run_url: str | None) -> T:
        return self

    async def register_as(self, model_name: str, model: Model) -> None:
        raise NotImplementedError(type(self))
