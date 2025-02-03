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

    def training_dataset(self, dataset: DatasetTypes | TrainingSet) -> ModelRegistryBuilder:
        self._input_example = dataset # type: ignore
        return self

    def feature_references(
        self, references: list[str] | list[FeatureReference] | ModelFeatureStore
    ) -> ModelRegistryBuilder:
        if isinstance(references, list) and isinstance(references[0], str):
            encodable_references = references # type: ignore
        else:
            from aligned.exposed_model.mlflow import reference_metadata_for_features
            from aligned.schemas.feature import FeatureReference

            if isinstance(references, ModelFeatureStore):
                references = references.input_features()

            if isinstance(references[0], FeatureReference):
                encodable_references = reference_metadata_for_features(references) # type: ignore
            else:
                encodable_references: list[str] = []
                for ref in references:
                    if isinstance(ref, FeatureReference):
                        encodable_references.append(ref.identifier)
                    else:
                        encodable_references.append(ref)

        self._metadata[ModelMetadata.feature_reference] = encodable_references
        return self

    def signature(self, signature: ModelSignature | dict | ModelFeatureStore) -> ModelRegistryBuilder:
        if isinstance(signature, ModelSignature):
            self._signature = signature
        elif isinstance(signature, dict):
            self._signature = ModelSignature.from_dict(signature)
        elif isinstance(signature, ModelFeatureStore):
            from aligned.exposed_model.mlflow import signature_for_contract
            self._signature = signature_for_contract(signature)
        else:
            raise NotImplementedError(f"Not supporting '{type(signature)}' yet")

        return self

    def training_run_url(self, run_url: str | None) -> ModelRegistryBuilder:
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
                pyfunc_predict_fn='predict',
                metadata=self._metadata,
                input_example=self._input_example
            )
        else:
            logger.info(
                f"Did not find a supported format. Will assume that '{type(model)}' it is a pyfunc model."
            )
            info = mlflow.pyfunc.log_model(
                python_model=model,
                artifact_path=model_name,
                registered_model_name=model_name,
                signature=self._signature,
                metadata=self._metadata,
                input_example=self._input_example
            )

        assert info
        assert info.registered_model_version

        for key, value in self._metadata.items():
            client.set_model_version_tag(
                model_name,
                version=str(info.registered_model_version),
                key=key,
                value=value
            )

def mlflow_registry() -> MlflowRegistryBuilder:
    return MlflowRegistryBuilder()
