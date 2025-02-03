from typing import Any, Protocol

import mlflow


class Tracker(Protocol):

    def log_metric(self, key: str, value: float, step: int | None) -> None:
        ...

    def log_param(self, key: str, value: float) -> None:
        ...

    def log_params(self, params: dict[str, Any]) -> None:
        ...


class MlflowTracker:

    run_id: str
    client: mlflow.MlflowClient

    def log_metric(self, key: str, value: float, step: int | None) -> None:
        self.client.log_metric(self.run_id, key, value, step=step)

    def log_param(self, key: str, value: float) -> None:
        self.client.log_param(self.run_id, key, value)

    def log_params(self, params: dict[str, Any]) -> None:
        for key, value in params.items():
            self.log_param(key, value)
