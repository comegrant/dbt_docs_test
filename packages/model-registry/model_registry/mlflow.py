from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import mlflow
from mlflow.models import ModelSignature
from mlflow.models.model import ModelInfo

from model_registry.interface import ModelMetadata, ModelRegistryBuilder

if TYPE_CHECKING:
    from aligned.feature_store import ModelFeatureStore
    from aligned.schemas.feature import FeatureReference
    from databricks.feature_store.training_set import TrainingSet

    from model_registry.interface import DatasetTypes, Model

logger = logging.getLogger(__name__)


@dataclass
class MlflowRegistryBuilder(ModelRegistryBuilder):

    _signature: ModelSignature | None = field(default=None)
    _metadata: dict = field(default_factory=dict)
    _input_example: DatasetTypes | None = field(default=None)
    _alias: str = field(default="challenger")

    def alias(self, alias: str) -> MlflowRegistryBuilder:
        self._alias = alias
        return self

    def training_dataset(self, dataset: DatasetTypes | TrainingSet) -> MlflowRegistryBuilder:
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

        self._metadata[ModelMetadata.feature_reference] = encodable_references
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
            info = mlflow.sklearn.log_model(
                model,
                artifact_path=model_name,
                registered_model_name=model_name,
                signature=self._signature,
                pyfunc_predict_fn="predict",
                metadata=self._metadata,
                input_example=self._input_example,
            )
        else:
            logger.info(f"Did not find a supported format. Will assume that '{type(model)}' it is a pyfunc model.")
            info = mlflow.pyfunc.log_model(
                python_model=model,
                artifact_path=model_name,
                registered_model_name=model_name,
                signature=self._signature,
                metadata=self._metadata,
                input_example=self._input_example,
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
