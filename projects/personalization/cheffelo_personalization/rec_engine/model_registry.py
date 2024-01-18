from dataclasses import dataclass, field

import mlflow

from cheffelo_personalization.rec_engine.clustering import ClusterModel
from cheffelo_personalization.rec_engine.content_based import CBModel


@dataclass
class ModelInfo:
    model_id: str
    model_name: str
    input_features: list[str] | None = field(default=None)


class ModelRegistry:
    def list_models(self) -> list[ModelInfo]:
        raise NotImplementedError(type(self))

    def store_model(self, model: ClusterModel | CBModel, model_id: str) -> None:
        raise NotImplementedError(type(self))

    def load_model(self, model_id: str) -> ClusterModel | CBModel | None:
        raise NotImplementedError(type(self))


@dataclass
class InMemoryModelRegistry(ModelRegistry):
    models: dict[str, ClusterModel | CBModel] = field(default_factory=dict)

    def list_models(self) -> list[ModelInfo]:
        return [ModelInfo(model_id, model_id) for model_id in self.models.keys()]

    def store_model(self, model: ClusterModel | CBModel, model_id: str) -> None:
        self.models[model_id] = model

    def load_model(self, model_id: str) -> ClusterModel | CBModel | None:
        return self.models.get(model_id)


@dataclass
class MlflowModelRegistry(ModelRegistry):
    def list_models(self) -> list[ModelInfo]:
        return [
            ModelInfo(model.latest_versions[0].source, model.name, None)
            for model in mlflow.search_registered_models()
            if model.latest_versions
        ]

    def store_model(self, model: ClusterModel | CBModel, model_id: str) -> None:
        mlflow.sklearn.save_model(model, model_id)

    def load_model(self, model_id: str) -> ClusterModel | CBModel | None:
        return mlflow.sklearn.load_model(model_id)
