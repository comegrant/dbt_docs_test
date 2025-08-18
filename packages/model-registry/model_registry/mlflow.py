from __future__ import annotations

import base64
import json
import logging
import zlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, overload

import mlflow
import polars as pl
from mlflow.models import ModelSignature
from mlflow.models.model import ModelInfo

from model_registry.interface import ModelMetadata, ModelRef, ModelRegistryBuilder
from model_registry.mlflow_infer import UnityCatalogColumn, features_for_uc, load_model, structure_feature_refs

if TYPE_CHECKING:
    import pandas as pd
    from aligned.feature_store import ModelFeatureStore
    from aligned.schemas.feature import FeatureReference

    from model_registry.interface import DatasetTypes, Model

logger = logging.getLogger(__name__)


@dataclass
class MlflowRegistryBuilder(ModelRegistryBuilder):
    _signature: ModelSignature | None = field(default=None)
    _metadata: dict[str, Any] = field(default_factory=dict)
    _input_example: DatasetTypes | None = field(default=None)
    _alias: str = field(default="challenger")

    tag_len_limit: int = field(default=1000)

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
        ref = ModelRef.from_string(model_uri)
        model, refs, output_name = load_model(ref, feature_refs, output_name)

        assert refs, (
            "Some feature refs are needed to use the model. "
            "Either log them through the model registry or manually pass them in the function."
        )

        structured_refs = structure_feature_refs(refs)

        if isinstance(entities, pl.LazyFrame):
            pd_ents = entities.collect().to_pandas()
        elif isinstance(entities, pl.DataFrame):
            pd_ents = entities.to_pandas()
        else:
            pd_ents = entities

        if not isinstance(structured_refs[0], UnityCatalogColumn):
            raise NotImplementedError("Currently only support inferring using Unity Catalog references.")

        features = features_for_uc(structured_refs, pd_ents)  # type: ignore
        pd_ents[output_name] = model.predict(features)

        if isinstance(entities, pl.LazyFrame):
            return pl.from_pandas(pd_ents).lazy()
        elif isinstance(entities, pl.DataFrame):
            return pl.from_pandas(pd_ents)
        else:
            return pd_ents

    def alias(self, alias: str) -> MlflowRegistryBuilder:
        self._alias = alias
        return self

    def training_dataset(self, dataset: DatasetTypes) -> MlflowRegistryBuilder:
        self._input_example = dataset  # type: ignore
        return self

    def feature_references(
        self, references: list[str] | list[FeatureReference] | ModelFeatureStore
    ) -> MlflowRegistryBuilder:
        if isinstance(references, list) and isinstance(references[0], str):
            encodable_references = references  # type: ignore
        else:
            from aligned.exposed_model.mlflow import reference_metadata_for_features
            from aligned.schemas.feature import FeatureReference

            if isinstance(references, ModelFeatureStore):
                references = references.input_features()

            if isinstance(references[0], FeatureReference):
                encodable_references = reference_metadata_for_features(references)  # type: ignore
            else:
                encodable_references: list[str] = []
                for ref in references:
                    if isinstance(ref, FeatureReference):
                        encodable_references.append(ref.identifier)
                    else:
                        encodable_references.append(ref)

        string_rep = str(encodable_references)
        if len(string_rep) >= self.tag_len_limit:
            compressed = zlib.compress(json.dumps(encodable_references).encode())
            string_rep = base64.b85encode(compressed).decode()

        self._metadata[ModelMetadata.feature_reference] = string_rep
        return self

    def signature(self, signature: ModelSignature | dict | ModelFeatureStore) -> MlflowRegistryBuilder:
        if isinstance(signature, ModelSignature):
            self._signature = signature
        elif isinstance(signature, dict):
            self._signature = ModelSignature.from_dict(signature)
        else:
            from aligned.feature_store import ModelFeatureStore

            if isinstance(signature, ModelFeatureStore):
                from aligned.exposed_model.mlflow import signature_for_contract

                self._signature = signature_for_contract(signature)
            else:
                raise NotImplementedError(f"Not supporting '{type(signature)}' yet")

        return self

    def training_run_url(self, run_url: str | None) -> MlflowRegistryBuilder:
        self._metadata[ModelMetadata.training_run_url] = run_url
        return self

    async def register_as(self, model_name: str, model: Model) -> None:
        from sklearn.base import BaseEstimator

        client = mlflow.MlflowClient()

        info: ModelInfo | None = None

        if isinstance(model, BaseEstimator):
            info = mlflow.sklearn.log_model(  # type: ignore
                model,
                artifact_path=model_name,
                registered_model_name=model_name,
                signature=self._signature,  # type: ignore
                pyfunc_predict_fn="predict",
                metadata=self._metadata,
                input_example=self._input_example,  # type: ignore
            )
        else:
            logger.info(f"Did not find a supported format. Will assume that '{type(model)}' it is a pyfunc model.")
            info = mlflow.pyfunc.log_model(
                python_model=model,
                artifact_path=model_name,
                registered_model_name=model_name,
                signature=self._signature,  # type: ignore
                metadata=self._metadata,
                input_example=self._input_example,  # type: ignore
            )

        assert info
        assert info.registered_model_version

        version = str(info.registered_model_version)
        client.set_registered_model_alias(model_name, self._alias, version)

        for key, value in self._metadata.items():
            client.set_model_version_tag(model_name, version=version, key=key, value=value)


def mlflow_registry() -> ModelRegistryBuilder:
    return MlflowRegistryBuilder()


def default_to_databricks_registry(username: str | None = None) -> ModelRegistryBuilder:
    """
    Returns a model registry that is connected to a MLFlow server.
    If no register URI is defined, will it try to connect to Databricks.

    Returns:
        A `ModelRegistryBuilder` that stores models
    """
    from mlflow.environment_variables import MLFLOW_REGISTRY_URI

    if MLFLOW_REGISTRY_URI.get() is None:
        logger.info("Setting up databricks registry")
        setup_databricks_model_registry(username)
    else:
        logger.info("Using registry described in env vars")

    return mlflow_registry()


def setup_databricks_model_registry(username: str | None = None) -> None:
    """
    Makes sure we have MLFlow configured correctly, and can load models from the Unity Catalog
    """
    import os

    use_uc_as_model_registry_key = "MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"
    token_key = "DATABRICKS_TOKEN"
    host_key = "DATABRICKS_HOST"

    if use_uc_as_model_registry_key not in os.environ:
        os.environ[use_uc_as_model_registry_key] = "true"

    if host_key not in os.environ:
        logger.warning(
            f"Unable to find environment variable for '{host_key}'."
            " This is needed in order to know which workspace to connect to."
        )

    if token_key not in os.environ:
        logger.warning(
            f"Unable to find environment variable for '{token_key}'."
            " This is needed in order to authenticate to databricks."
        )

    mlflow.set_registry_uri("databricks-uc")
    if username is None:
        mlflow.set_tracking_uri("databricks")
    else:
        mlflow.set_tracking_uri(f"databricks://{username}")


def databricks_model_registry(username: str | None = None) -> ModelRegistryBuilder:
    setup_databricks_model_registry(username)
    return mlflow_registry()
