from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import mlflow
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store.training_set import TrainingSet

from model_registry.interface import ModelRegistryBuilder

if TYPE_CHECKING:
    from model_registry.interface import DatasetTypes, Model


logger = logging.getLogger(__name__)


@dataclass
class DatabricksFeatureStoreRegistryBuilder(ModelRegistryBuilder):
    _dataset: TrainingSet | None = field(default=None)

    def training_dataset(self, dataset: DatasetTypes | TrainingSet) -> DatabricksFeatureStoreRegistryBuilder:
        if isinstance(dataset, TrainingSet):
            self._dataset = dataset
        else:
            logger.info(f"The Databricks Feature store supports only TrainingSet as the dataset. Got '{type(dataset)}'")
        return self

    async def register_as(self, model_name: str, model: Model) -> None:
        from sklearn.base import BaseEstimator

        flavor = None
        if isinstance(model, BaseEstimator):
            flavor = mlflow.sklearn
        elif isinstance(model, mlflow.models.PythonModel):  # type: ignore
            flavor = mlflow.pyfunc

        assert flavor

        client = FeatureStoreClient()
        client.log_model(model, model_name, flavor=flavor, training_set=self._dataset, registered_model_name=model_name)


def databricks_feature_store() -> DatabricksFeatureStoreRegistryBuilder:
    return DatabricksFeatureStoreRegistryBuilder()
