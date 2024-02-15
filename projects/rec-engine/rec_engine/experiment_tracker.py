from dataclasses import dataclass
from logging import Logger
from typing import Protocol
from uuid import uuid4

import streamlit as st
from mlflow.client import MlflowClient
from pandas import DataFrame


class ExperimentTracker(Protocol):
    def log_metric(self, name: str, value: float, step: int | None = None):
        ...

    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        ...

    def log_dataframe(self, dataframe: DataFrame, name: str):
        ...


@dataclass
class MlflowTracker(ExperimentTracker):
    run_id: str
    client: MlflowClient

    def log_metric(self, name: str, value: float, step: int | None = None):
        return self.client.log_metric(self.run_id, name, value, step=step)

    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        for name, value in metrics.items():
            self.log_metric(name, value, step)

    def log_dataframe(self, dataframe: DataFrame, name: str):
        self.client.log_table(self.run_id, dataframe, artifact_file=name)

    @staticmethod
    def start_run(
        run_name: str, tags: dict[str, str] | None = None, client: MlflowClient | None = None,
    ) -> "MlflowTracker":
        client = client or MlflowClient()
        experiment_id = str(uuid4())
        run = client.create_run(experiment_id, run_name=run_name, tags=tags)
        return MlflowTracker(run.info.run_id, client)


@dataclass
class LoggerTracker(ExperimentTracker):
    logger: Logger

    def log_metric(self, name: str, value: float, step: int | None = None):
        self.logger.info(f"{name} - step {step}: {value}")

    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        for name, value in metrics.items():
            self.log_metric(name, value, step)

    def log_dataframe(self, dataframe: DataFrame, name: str):
        self.logger.info(f"Dataframe - {name}: {dataframe.to_markdown()}")


@dataclass
class StreamlitTracker(ExperimentTracker):
    def log_metric(self, name: str, value: float, step: int | None = None):
        st.metric(name, value)

    def log_metrics(self, metrics: dict[str, float], step: int | None = None):
        for name, value in metrics.items():
            self.log_metric(name, value, step)

    def log_dataframe(self, dataframe: DataFrame, name: str):
        st.write(name)
        st.dataframe(dataframe)
