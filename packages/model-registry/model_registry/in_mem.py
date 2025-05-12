from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mlflow.models import ModelSignature

from model_registry.interface import ModelRegistryBuilder
from model_registry.mlflow import MlflowRegistryBuilder

if TYPE_CHECKING:
    from aligned.feature_store import ModelFeatureStore
    from aligned.schemas.feature import FeatureReference
    from databricks.feature_store.training_set import TrainingSet

    from model_registry.interface import DatasetTypes, Model

logger = logging.getLogger(__name__)


@dataclass
class InMemoryRegistry(ModelRegistryBuilder):
    """
    A model registry used for testing purposes, when using dependency injection.
    """

    registry: MlflowRegistryBuilder = field(default_factory=MlflowRegistryBuilder)

    model_name: str | None = None
    model: Model | None = None

    def alias(self, alias: str) -> ModelRegistryBuilder:
        self.registry = self.registry.alias(alias)
        return self

    def training_dataset(self, dataset: DatasetTypes | TrainingSet) -> ModelRegistryBuilder:
        self.registry = self.registry.training_dataset(dataset)
        return self

    def feature_references(
        self, references: list[str] | list[FeatureReference] | ModelFeatureStore
    ) -> ModelRegistryBuilder:
        self.registry = self.registry.feature_references(references)
        return self

    def signature(self, signature: ModelSignature | dict | ModelFeatureStore) -> ModelRegistryBuilder:
        self.registry = self.registry.signature(signature)
        return self

    def training_run_url(self, run_url: str | None) -> ModelRegistryBuilder:
        self.registry = self.registry.training_run_url(run_url)
        return self

    async def register_as(self, model_name: str, model: Model) -> None:
        logger.info(f"Storing '{model_name}' in mem.")
        self.model_name = model_name
        self.model = model
