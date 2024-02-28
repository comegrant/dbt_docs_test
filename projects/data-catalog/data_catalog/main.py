# Copied from https://github.com/MatsMoll/aligned-streamlit/blob/main/aligned_streamlit/data_catalog_ui.py
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import polars as pl
import streamlit as st
from aligned import FeatureStore
from aligned.data_file import DataFileReference
from aligned.feature_source import WritableFeatureSource
from aligned.feature_store import FeatureViewStore, ModelFeatureStore
from aligned.retrival_job import RetrivalJob
from aligned.schemas.constraints import Optional
from aligned.schemas.feature import (
    EventTimestamp,
    Feature,
    FeatureLocation,
    FeatureReferance,
    FeatureType,
)
from aligned.schemas.feature_view import CompiledFeatureView, RetrivalRequest
from aligned.schemas.model import (
    ClassificationTarget,
    FeatureInputVersions,
    RegressionTarget,
)
from aligned.schemas.repo_definition import RepoDefinition
from aligned.schemas.transformation import Transformation
from data_contracts.recommendations.store import recommendation_feature_contracts
from dotenv import load_dotenv
from streamlit.delta_generator import DeltaGenerator

logger = logging.getLogger(__name__)


async def use_data_managment_features() -> bool:
    from os import getenv

    return getenv("USE_DATA_MANAGMENT_FEATURES", "false") == "true"


def show_input_features(features: set[FeatureReferance] | FeatureInputVersions) -> None:
    feature_list = []
    if isinstance(features, FeatureInputVersions):
        keys = list(features.versions.keys())
        default_index = keys.index(features.default_version)
        selected_version = st.selectbox("Select Feature Version", keys, index=default_index)
        feature_list = features.features_for(selected_version)
    else:
        feature_list = list(features)

    st.subheader("Input Features")
    for feature in feature_list:
        feature_reference_cell(feature)


def overview_graph(store: FeatureStore):
    nodes, edges = feature_view_overview_graph(store)
    nodes_model, edges_model = model_overview_graph(store)

    nodes = nodes.union(nodes_model)
    edges = edges.union(edges_model)

    for model in store.models.values():
        refs = model.feature_references()
        for ref in refs:
            edges.add(
                GraphEdge(ref.location.identifier, FeatureLocation.model(model.name).identifier),
            )

    return nodes, edges


def model_overview_graph(store: FeatureStore):
    nodes = set()
    edges = set()

    for model_name in store.models:
        loc = FeatureLocation.model(model_name)

        nodes.add(GraphNode(model_name, loc.identifier, "model"))
        for dep in store.model(model_name).depends_on():
            if dep.location == "model":
                edges.add(GraphEdge(dep.identifier, loc.identifier))

    return nodes, edges


def feature_view_overview_graph(store: FeatureStore):
    nodes = set()
    edges = set()

    for view_name, view in store.feature_views.items():
        loc = FeatureLocation.feature_view(view_name)
        nodes.add(GraphNode(loc.name, loc.identifier, "feature_view"))

        for dep in view.source.depends_on():
            if dep.location == "feature_view":
                edges.add(GraphEdge(dep.identifier, loc.identifier))

    return nodes, edges


def feature_view_overview(store: FeatureStore):
    st.subheader("Feature Views")

    nodes, edges = feature_view_overview_graph(store)
    show_mermiad_graph("Feature View Overview Graph", nodes, edges, graph_type="TB")


def model_contracts_overview(store: FeatureStore):
    st.subheader("Model Contracts")

    classification_contracts = []
    regression_contracts = []
    other_contracts = []

    for model_name, model in store.models.items():
        pred_view = model.predictions_view

        if pred_view.classification_targets:
            classification_contracts.append(model_name)
        elif pred_view.regression_targets:
            regression_contracts.append(model_name)
        else:
            other_contracts.append(model_name)

    clasif, reg, other = st.columns(3)

    clasif.metric("Classification", len(classification_contracts))
    reg.metric("Regression", len(regression_contracts))
    other.metric("Other", len(other_contracts))

    nodes, edges = model_overview_graph(store)
    show_mermiad_graph("Model Overview Graph", nodes, edges)

    with st.expander("Classification Contracts"):
        for contract in classification_contracts:
            st.write(contract)

    with st.expander("Regression Contracts"):
        for contract in regression_contracts:
            st.write(contract)

    with st.expander("Other Contracts"):
        for contract in other_contracts:
            st.write(contract)


async def show_model_contract(store: FeatureStore, contract_name: str):
    contract_store = store.model(contract_name)
    contract = contract_store.model
    pred_view = contract.predictions_view

    st.title(contract_name)

    main_col, side_col = st.columns([2, 1])

    with side_col:
        with st.container(border=True):
            st.subheader("Metadata")

            st.caption("Exposed at")
            if contract.exposed_at_url:
                st.write(f"Model can be found at: `{contract.exposed_at_url}`")
                st.link_button("Go to API", contract.exposed_at_url)
            else:
                st.write("No API exposed, it can be set with `exposed_at_url`")
                st.code(
                    f"""
                @model_contract(
                    name="{contract.name}",
                    exposed_at_url="https://api.example.com"
                )
                class {contract.name}:
                    ...
                """,
                )

            if contract.description:
                st.write(contract.description)

            if contract.tags:
                st.write(contract.tags)

            if contract.contacts:
                st.subheader("Contacts")
                for contact in contract.contacts:
                    st.write(contact)

        with st.expander("Stored At"):
            st.subheader("Stored At")
            st.write(pred_view.source)

    with main_col:
        with st.expander("Model Prediction Schema", expanded=True):
            st.subheader("Output features")
            output_features = pred_view.features.union(pred_view.derived_features)
            for feature in output_features:
                feature_cell(feature)

            st.subheader("Entities")
            st.write(
                "To uniquely identify a row in the view, you need to provide the following entities",
            )
            for entity in pred_view.entities:
                feature_cell(entity)

            if pred_view.event_timestamp:
                st.subheader("Event Timestamp")
                st.write(
                    "The timestamp describing when the feature was valid from. This can be set to travel back in time. This will also be used to compute the freshness of a view.",
                )
                feature_cell(pred_view.event_timestamp)

        with st.expander("Input Features"):
            show_input_features(contract.features)

    st.subheader("Data Management")
    with st.container(border=True):
        load_input_features, evaluate_models, view_predictions, dataset_list = st.tabs(
            ["Load Input Features", "Evaluate Model", "View Predictions", "Datasets"],
        )

        with load_input_features:
            await model_get_input_features(contract_store)

        if pred_view.regression_targets:
            with st.expander("Regression Targets"):
                st.subheader("Regression Targets")
                for feature in pred_view.regression_targets:
                    st.write("Target feature")
                    feature_reference_cell(feature.estimating)

                    st.write("Predicted feature")
                    feature_cell(feature.feature)

                    if feature.confidence:
                        st.write("Confidence feature")
                        feature_cell(feature.confidence)
                    else:
                        st.write("No confidence feature set")

                    if feature.lower_confidence:
                        st.write("Lower confidence feature")
                        feature_cell(feature.lower_confidence)
                    else:
                        st.write("No lower confidence feature set")

                    if feature.upper_confidence:
                        st.write("Upper confidence feature")
                        feature_cell(feature.upper_confidence)
                    else:
                        st.write("No upper confidence feature set")

        if not pred_view.source:
            st.warning("No prediction source is set, so no predictions can be explored.")
            return

        with dataset_list:
            await model_datasets(contract_store)

        with view_predictions:
            st.subheader("Predictions")

            limit = st.number_input("Number of predictions", value=100)
            if st.button("Load Predictions"):
                with st.spinner("Loading..."):
                    predictions = (
                        await contract_store.all_predictions(limit=int(limit)).to_polars()
                    ).collect()

                st.dataframe(predictions.to_pandas())

        target = None
        if pred_view.regression_targets:
            target = list(pred_view.regression_targets)[0]
        elif pred_view.classification_targets:
            target = list(pred_view.classification_targets)[0]

        if not target:
            st.warning("No target is set, so no evaluation can be done.")
            return

        with evaluate_models:
            st.subheader("Evaluate")

            needed_entities = contract_store.needed_entities()
            needed_entitie_names = {ent.name for ent in needed_entities}

            for entity in needed_entities:
                if entity.name not in needed_entitie_names:
                    st.warning(
                        f"Entities `{needed_entitie_names}` is needed, but some are missing in the current prediction view schema.",
                    )
                    return

            await evaluate_model_contract(store, contract_name, target)


async def model_datasets(model_store: ModelFeatureStore):
    model = model_store.model
    if not model.dataset_store:
        st.warning("No dataset store is set, so no baseline model compareson can be done.")
        return

    all_datasets = await model.dataset_store.list_datasets()

    if not all_datasets:
        st.warning("No datasets found.")
        return

    for dataset in all_datasets.all:
        st.write(dataset.name)


async def evaluate_model_contract(
    store: FeatureStore,
    contract_name: str,
    target: RegressionTarget | ClassificationTarget,
):
    contract_store = store.model(contract_name)
    contract = contract_store.model
    pred_view = contract.predictions_view

    entities = [ent.name for ent in pred_view.entities]
    entities_names = [ent.name for ent in pred_view.entities]
    if pred_view.event_timestamp:
        entities_names.append(f"selected_{pred_view.event_timestamp.name}")

    target_ref = target.estimating

    st.write("#### How it works")
    st.write(
        """Evaluation will be done by loading all the predictions in the model contract (aka. the `prediction_source`.

The predictions will then join the ground truth, based on the target. Either by using `.as_regression_target()` or `.as_classification_label()`.
We will filter out the predictions where the target is set to `None`.

If the model contract have a `model_version_column` set, then we will evaluate each model version separately.
    """,
    )

    with st.form("Evaluate Predictions"):
        nr_cols = st.number_input("Number of columns", value=1)

        st.write("#### Used Ground Truth Target")
        location_type = st.selectbox("Location Type", ["feature_view", "model"], index=0)
        location_name = st.text_input("Name", value=target_ref.location.name)
        feature_name = st.text_input("Feature", value=target_ref.name)

        should_evaluate = st.form_submit_button("Evaluate")

    if not should_evaluate:
        return

    if location_type and location_name and feature_name:
        target_ref = FeatureReferance(
            feature_name,
            FeatureLocation(location_name, location_type),
            target_ref.dtype,
        )

    if isinstance(target, ClassificationTarget):
        baseline_models = [
            ColumnWrapper.train_constructor(store, "transactions:granular_heuristic"),
            MostCommonModel.train_constructor(target.estimating.name),
            ClassProbabilityModel.train_constructor(target.estimating.name, top_n=5),
        ]
    else:
        baseline_models = [
            MeanPerGroupModel.train_constructor(group_by=entities, y_column=target.estimating.name),
            MeanModel.train_constructor(entities, target.estimating.name),
        ]

    # if contract.evaluation:
    #
    #
    #
    #         preds_job,
    #         contract.evaluation,
    #         store,
    #         contract_name,
    #
    #     if not models:
    #
    #     for model in models:
    #         await evaluate_model(
    #             RetrivalJob.from_convertable(
    #                 preds.with_columns(
    #
    #                 ),
    #                 preds_job.retrival_requests
    #             ),
    #             contract.evaluation,
    #             store,
    #             contract_name,
    #             metrics
    #
    #
    #
    with st.spinner("Loading predictions..."):
        preds = await contract_store.all_predictions().to_polars()

        if pred_view.event_timestamp:
            preds = preds.rename(
                {pred_view.event_timestamp.name: f"selected_{pred_view.event_timestamp.name}"},
            )

        preds = preds.sort(entities_names).collect()

    st.write(f"Loaded **{preds.height}** predictions")
    st.write(f"Joining ground truth on `{target_ref.identifier}`")

    with st.spinner("Loading targets..."):
        target_job = store.features_for(
            preds,
            features=[target_ref.identifier],
            event_timestamp_column=f"selected_{pred_view.event_timestamp.name}"
            if pred_view.event_timestamp
            else None,
        ).log_each_job()
        targets = (await target_job.to_polars()).sort(entities_names).collect()

    with st.expander("Target"):
        st.write(f"Unfiltered targets have **{targets.height}** rows")
        st.dataframe(targets.to_pandas())

    targets = targets.drop_nulls([target_ref.name])

    with st.expander("Target"):
        st.write(f"Unfiltered targets have **{targets.height}** rows")
        st.dataframe(targets.to_pandas())

    if isinstance(target, ClassificationTarget):
        unique_labels = sorted(targets[target_ref.name].unique().to_list())
        with st.form("Select Labels"):
            selected_labels = st.multiselect("Select Labels", unique_labels, default=unique_labels)
            st.form_submit_button()

        targets = targets.filter(pl.col(target_ref.name).is_in(selected_labels))

    with st.expander("Unfiltered Predictions"):
        st.write(f"Unfiltered predictions have **{preds.height}** rows")
        st.dataframe(preds.to_pandas())

    preds = preds.filter(
        pl.concat_str(entities).is_in(targets.select(pl.concat_str(entities).alias("key"))["key"]),
    )

    with st.expander("Prediction"):
        st.write(f"Unfiltered predictions have **{preds.height}** rows")
        st.dataframe(preds.to_pandas())

    st.write(f"Evaluating using **{preds.height}** predictions")

    cols = [st]
    if nr_cols > 1:
        cols = st.columns(int(nr_cols))

    if pred_view.model_version_column:
        model_versions = preds[pred_view.model_version_column.name].unique().sort().to_list()
        st.subheader("Model Versions")

        for index, model_version in enumerate(model_versions):
            col = cols[index % len(cols)]

            with col.expander(f"{model_version}"):
                col.subheader(model_version)
                preds.select(
                    is_model=pl.col(pred_view.model_version_column.name) == model_version,
                )["is_model"].arg_true()

                # if isinstance(target, RegressionTarget):
                #     evaluate_regression_view(
                #

        #     ent.name for ent in pred_view.entities
        # ]).agg([
        # ]).with_columns(
        # ).with_columns(
        # ).sort(
        # ).head(100)

    #     if isinstance(target, RegressionTarget):
    #         evaluate_regression_view(
    #

    st.subheader("Compare against baseline models")
    models = await train_baseline_model(
        model_store=contract_store,
        target=target,
        baseline_model=baseline_models,
    )
    if not models:
        return

    target_cols = list(targets.columns)
    target_cols.remove(target_ref.name)

    baseline_target = targets.unique(entities_names)

    cols = [st]
    if nr_cols > 1:
        cols = st.columns(int(nr_cols))

    for index, model in enumerate(models):
        col = cols[index % len(cols)]
        col.subheader(f"Baseline Model {type(model).__name__}")
        preds = pl.Series(await model.predict(baseline_target[target_cols].to_pandas()))

        # if isinstance(target, ClassificationTarget):
        #     evaluate_classification_view(
        #         preds,
        #     evaluate_regression_view(
        #         preds,
        #


class BaselineModel:
    @staticmethod
    async def train(entities: pd.DataFrame) -> BaselineModel:
        raise NotImplementedError()

    async def predict(self, x: pd.DataFrame) -> pd.Series:
        raise NotImplementedError()


@dataclass
class MeanPerGroupModel(BaselineModel):
    group_by: list[str]
    y_column: str | None = None
    mean_per_group: pd.DataFrame | None = None

    @staticmethod
    def train_constructor(
        group_by: list[str],
        y_column: str,
    ) -> Callable[[pl.DataFrame], Coroutine[None, None, BaselineModel]]:
        async def train(entities: pl.DataFrame) -> BaselineModel:
            return await MeanPerGroupModel.train(entities, y_column, group_by)

        return train

    @staticmethod
    async def train(
        entities: pl.DataFrame,
        y_column: str,
        group_by: list[str],
    ) -> MeanPerGroupModel:
        mean_per_group = (
            entities.to_pandas().groupby(group_by).mean().reset_index()[[*group_by, y_column]]
        )

        return MeanPerGroupModel(
            group_by=group_by,
            y_column=y_column,
            mean_per_group=mean_per_group,
        )

    async def predict(self, x: pd.DataFrame) -> pd.Series:
        return x.merge(self.mean_per_group, on=self.group_by, how="left").fillna(0)[self.y_column]


@dataclass
class ColumnWrapper(BaselineModel):
    store: FeatureStore
    feature_ref: str

    async def predict(self, X: pd.DataFrame) -> pd.Series:
        column = self.feature_ref.split(":")[1]
        return (await self.store.features_for(X, [self.feature_ref]).to_pandas())[column].fillna(
            "Unknown",
        )

    @staticmethod
    def train_constructor(
        store: FeatureStore,
        feature_ref: str,
    ) -> Callable[[pl.DataFrame], Coroutine[None, None, BaselineModel]]:
        async def train(entities: pl.DataFrame) -> BaselineModel:
            return ColumnWrapper(store, feature_ref)

        return train


@dataclass
class MostCommonModel(BaselineModel):
    most_common_class: str

    @staticmethod
    def train_constructor(
        y_column: str,
    ) -> Callable[[pl.DataFrame], Coroutine[None, None, BaselineModel]]:
        async def train(entities: pl.DataFrame) -> BaselineModel:
            most_common = entities[y_column].mode()[0]
            return MostCommonModel(most_common)

        return train

    def train(self, x: pd.DataFrame, y: pd.DataFrame) -> None:
        pass

    async def predict(self, x: pd.DataFrame) -> pd.Series:
        return pd.Series([self.most_common_class] * len(x))


@dataclass
class ClassProbabilityModel(BaselineModel):
    probability: pd.DataFrame
    y_column: str

    @staticmethod
    def train_constructor(
        y_column: str,
        top_n: int | None,
    ) -> Callable[[pl.DataFrame], Coroutine[None, None, BaselineModel]]:
        async def train(entities: pl.DataFrame) -> BaselineModel:
            probs = entities[y_column].value_counts()
            if top_n:
                probs = probs.sort("count", descending=True).head(top_n)

            return ClassProbabilityModel(probs, y_column)

        return train

    async def predict(self, x: pd.DataFrame) -> pd.Series:
        import random

        return pd.Series(
            random.choices(
                self.probability[self.y_column].unique(),
                weights=self.probability["count"].to_list(),
                k=len(x),
            ),
        )


class MeanModel(BaselineModel):
    mean: float

    @staticmethod
    def train_constructor(
        entity_names: list[str],
        y_column: str,
    ) -> Callable[[pl.DataFrame], Coroutine[None, None, BaselineModel]]:
        async def train(entities: pl.DataFrame) -> BaselineModel:
            model = MeanModel()
            model.train(entities[entity_names].to_pandas(), entities[y_column].to_pandas())
            return model

        return train

    def train(self, x: pd.DataFrame, y: pd.DataFrame) -> None:
        mean = y.mean()
        if isinstance(mean, pd.Series):
            self.mean = mean[0]
        else:
            self.mean = mean

    async def predict(self, x: pd.DataFrame) -> pd.Series:
        return pd.Series([self.mean] * len(x))


class MedianModel(BaselineModel):
    median: float

    def train(self, x: pd.DataFrame, y: pd.DataFrame) -> None:
        median = y.median()

        if isinstance(median, pd.Series):
            self.median = median[0]
        else:
            self.median = median

    async def predict(self, x: pd.DataFrame) -> pd.Series:
        return pd.Series([self.median] * len(x))


async def train_baseline_model(
    model_store: ModelFeatureStore,
    target: RegressionTarget | ClassificationTarget,
    baseline_model: list[Callable[[pl.DataFrame], Coroutine[None, None, BaselineModel]]],
) -> list[BaselineModel] | None:
    if not model_store.model.dataset_store:
        st.warning("No dataset store is set, so no baseline model compareson can be done.")
        return

    target_column = target.estimating.name

    all_datasets = await model_store.model.dataset_store.list_datasets()
    used_datasets = all_datasets.train_test + all_datasets.train_test_validation
    datasets = [dataset.id for dataset in used_datasets]

    with st.form("Select Dataset"):
        select_dataset = st.selectbox("Select Dataset", datasets, index=None)

        st.form_submit_button("Train Baseline Model")

    if not select_dataset:
        st.warning("No dataset selected.")
        return

    index_of_dataset = datasets.index(select_dataset)
    dataset = used_datasets[index_of_dataset]

    result = dataset.request_result

    with st.spinner("Loading data..."):
        data = (await dataset.train_dataset.all(result=result).to_polars()).collect()

    input_columns = list(data.columns)
    if target_column in input_columns:
        input_columns.remove(target_column)

    models = []
    for model in baseline_model:
        models.append(await model(data))
    return models


async def test_transform_for(view: FeatureViewStore):
    st.subheader("Test Transformations")
    st.write("Test how the transform will work on a small dataset.")

    if await use_data_managment_features():
        st.warning(
            "Data managment features are not enabled. Please enable them to use this feature.",
        )
        return

    with st.form("Features To Transform"):
        feature_refs = [
            feat
            for feat in view.request.all_returned_columns
            if (feat not in view.request.feature_names) and (feat not in view.request.entity_names)
        ]
        features = st.multiselect("Features", feature_refs, default=feature_refs)
        st.form_submit_button("Set Input Features")

    if not features:
        st.warning("No features selected.")
        return

    request = view.view.request_for(set(features)).needed_requests[0]

    def ref_for_feature(feature: Feature) -> FeatureReferance:
        return FeatureReferance(
            feature.name,
            FeatureLocation.feature_view(view.name),
            feature.dtype,
        )

    input_features = {ref_for_feature(feature) for feature in request.entities}

    for feature in request.derived_features:
        input_features.update(feature.depending_on)

    for feature in request.aggregated_features:
        input_features.update(feature.derived_feature.depending_on)

    aggregation = request.aggregate_over()

    need_event_timestamp = False

    for over, features in aggregation.items():
        if over.window:
            need_event_timestamp = True

    if need_event_timestamp:
        if not view.view.event_timestamp:
            st.warning("Need event timestamp, but none is set.")
            return

        input_features.add(
            FeatureReferance(
                view.view.event_timestamp.name,
                FeatureLocation.feature_view(view.name),
                view.view.event_timestamp.dtype,
            ),
        )

    core_features = request.feature_names

    input_features = {feat for feat in input_features if feat.name in core_features}

    st.subheader("Choose Input Features")
    with st.form("Input Features"):
        input_df = st.data_editor(
            pd.DataFrame(columns=[feat.name for feat in input_features]),
            num_rows="dynamic",
        )
        st.form_submit_button()

    if input_df.empty:
        st.warning("Need at least one entity row.")
        return

    ensure_types_req = RetrivalRequest(
        "features",
        FeatureLocation.feature_view("test"),
        set(),
        {Feature(feat.name, feat.dtype) for feat in input_features},
        set(),
    )

    if need_event_timestamp:
        input_df[view.view.event_timestamp.name] = pd.to_datetime(
            input_df[view.view.event_timestamp.name],
        )

    ensure_job = (
        RetrivalJob.from_convertable(input_df, request)
        .ensure_types([ensure_types_req])
        .with_request([request])
    )

    with st.spinner("Loading..."):
        data = (await ensure_job.aggregate(request).derive_features().to_polars()).collect()

    st.subheader("Transformed Features")
    st.dataframe(data.to_pandas())


async def model_get_input_features(model_store: ModelFeatureStore) -> None:
    if isinstance(model_store.model.features, FeatureInputVersions):
        keys = list(model_store.model.features.versions.keys())
        default_index = keys.index(model_store.model.features.default_version)
        selected_version = st.selectbox(
            "Select Feature Version",
            keys,
            index=default_index,
            key="Select version for features",
        )
        model_store = model_store.using_version(selected_version)

    needed_columns = [ent.name for ent in model_store.needed_entities()]

    event_timestamp = None
    if model_store.model.predictions_view.event_timestamp:
        event_timestamp = "at_point_in_time"
        needed_columns.append(event_timestamp)

    st.subheader("Depends On")

    for dep in model_store.depends_on():
        with st.container(border=True):
            feature_location(dep)

    with st.form("Set Entities"):
        st.subheader("Select who to load for")
        entities = st.data_editor(pd.DataFrame(columns=needed_columns), num_rows="dynamic")

        st.form_submit_button()

    if entities.empty:
        st.warning("Need at least one entity row.")
        return

    if event_timestamp:
        entities[event_timestamp] = pd.to_datetime(entities[event_timestamp])

    with st.spinner("Loading..."):
        data = (
            await model_store.features_for(
                entities,
                event_timestamp_column=event_timestamp,
            ).to_polars()
        ).collect()

    st.subheader("Loaded features")
    st.dataframe(data.to_pandas())


# async def evaluate_model(
#     prediction: RetrivalJob,
#     evaluation: ModelEvaluation,
#     store: FeatureStore,
#     model_name: str,
# ) -> dict[str, float]:
#     if baseline_metrics is None:
#
#
#
#
#     for metric in metrics:
#         if isinstance(metric, SingleMetric):
#
#
#     if single_metrics:
#         for index, metric in enumerate(single_metrics):
#
#             if metric.name in baseline_metrics:
#
#             cols[index % len(cols)].metric(
#                 metric.name,
#
#     if figures:
#         for index, figure in enumerate(figures):
#

# def evaluate_classification_view(
#     prediction: pl.Series,
#     target: pl.Series,
#     col: DeltaGenerator,
# ) -> dict[str, float]:
#
#     if baseline_metrics is None:
#
#     if metrics is None:
#
#
#     if figures is None:
#                 "Confusion Matrix",
#                 lambda pred, target: ConfusionMatrixDisplay.from_predictions(
#                 ).plot(
#                 ).figure_
#             ),
#
#     if len(metrics) > 0:
#         for index, metric in enumerate(metrics):
#
#             if pd.isna(value):
#
#             if metric.name in baseline_metrics:
#
#             cols[index % len(cols)].metric(
#                 metric.name,
#
#     if figures:
#         for index, (name, figure) in enumerate(figures):
#


# def evaluate_regression_view(
#     prediction: pl.Series,
#     target: pl.Series,
# ):
#
#     if metrics is None:
#
#     if figures is None:
#                 "Actual vs. predicted",
#                 lambda pred, target: PredictionErrorDisplay(
#                 ).plot(
#                 ).figure_
#             ),
#                 "Residual vs. predicted",
#                 lambda pred, target: PredictionErrorDisplay(
#                 ).plot(
#                 ).figure_
#             ),
#
#
#     if len(metrics) > 0:
#         for index, metric in enumerate(metrics):
#             cols[index % len(cols)].metric(
#                 metric.name,
#
#
#     if figures:
#         for index, (name, figure) in enumerate(figures):


@dataclass
class GraphNode:
    def __hash__(self) -> int:
        return hash(self.id)

    name: str
    id: str
    node_type: str


@dataclass
class GraphEdge:
    def __hash__(self) -> int:
        return hash((self.from_node, self.to_node))

    from_node: str
    to_node: str


def inter_feature_view_nodes(view: CompiledFeatureView) -> tuple[set[GraphNode], set[GraphEdge]]:
    view_location = FeatureLocation.feature_view(view.name)

    nodes = set()
    edges = set()

    def node_identifier(name: str, location: FeatureLocation) -> str:
        return f"{location.identifier}:{name}"

    def feature_node(name: str, location: FeatureLocation = view_location) -> GraphNode:
        return GraphNode(name, node_identifier(name, location), "feature")

    for entity in view.entities:
        nodes.add(feature_node(entity.name))

    for feature in view.features:
        nodes.add(feature_node(feature.name))

    for feature in view.derived_features.union(
        {feature.derived_feature for feature in view.aggregated_features},
    ):
        node = feature_node(feature.name)
        nodes.add(node)

        for dep in feature.depending_on:
            dep_id = node_identifier(dep.name, dep.location)
            edges.add(GraphEdge(dep_id, node.id))
            nodes.add(feature_node(dep.name, dep.location))

    return nodes, edges


def mermiad_graph(nodes: set[GraphNode], edges: set[GraphEdge], graph_type: str = "LR") -> str:
    def format_node(node: GraphNode) -> str:
        if node.node_type == "model":
            if graph_type == "LR":
                return f"{node.id}>{node.name}]"
            else:
                return f"{node.id}[\\{node.name}/]"
        else:
            return f"{node.id}([{node.name}])"

    node_strings = ";\n".join([format_node(node) for node in nodes])
    edge_strings = ";\n".join([f"{edge.from_node} --> {edge.to_node}" for edge in edges])

    return f"""
        graph {graph_type}
        {node_strings};
        {edge_strings};
    """


def feature_reference_cell(feature: FeatureReferance):
    with st.container(border=True):
        left, right = st.columns(2)
        left.text(feature.name)
        right.caption(feature.dtype.name)

        st.caption("Located at")

        left, right = st.columns(2)
        left.text(feature.location.name)
        right.caption(feature.location.location)


def feature_cell(feature: Feature, col: DeltaGenerator | None = None):
    if col is None:
        col = st

    with col.container(border=True):
        left, right = st.columns(2)
        left.text(feature.name)
        right.caption(feature.dtype.name)

        if feature.description:
            st.text(feature.description)

        if isinstance(feature, EventTimestamp):
            return

        if feature.constraints and Optional() in feature.constraints:
            st.caption("Is Optional")


async def manually_show_features(store: FeatureStore, view_name: str):
    if await use_data_managment_features():
        st.warning(
            "Data managment features are not enabled. Please enable them to use this feature.",
        )
        return

    view_store = store.feature_view(view_name)
    view = view_store.view

    entities = view.entitiy_names
    event_timestamp = None

    if view.event_timestamp:
        event_timestamp = view.event_timestamp.name
        entities.add(event_timestamp)

    st.write("Load data based on a set of entities.")

    with st.form("Set Entities"):
        st.subheader("Select who to load for")
        entities = st.data_editor(
            pd.DataFrame(
                columns=list(entities),
            ),
            num_rows="dynamic",
        )
        st.form_submit_button()

    if entities.empty:
        st.warning("Need at least one entity row.")
        return

    if event_timestamp:
        entities[event_timestamp] = pd.to_datetime(entities[event_timestamp])

    features = (
        await view_store.features_for(entities, event_timestamp_column=event_timestamp)
        .log_each_job()
        .to_polars()
    ).collect()

    st.subheader("Loaded features")
    st.dataframe(features.to_pandas())


async def load_feature_view_data(store: FeatureStore, view_name: str):
    st.write("Load data from the source, and understand how it behaves.")

    if await use_data_managment_features():
        st.warning(
            "Data managment features are not enabled. Please enable them to use this feature.",
        )
        return

    view_store = store.feature_view(view_name)

    all_data, between_data = st.tabs(["Load a limit", "Load Between Dates"])

    with all_data:
        st.write("Preview a limit of data")
        limit = st.number_input("Limit", value=100, key=f"{view_name}-load_limit_preview")
        all_features = sorted(view_store.request.all_feature_names)
        features = st.multiselect(
            "Features",
            all_features,
            default=all_features,
            key=f"{view_name}-load_features_preview",
        )

        if st.button("Load data", key="load_limit_preview"):
            start_time = datetime.now()
            with st.spinner("Loading..."):
                data = (
                    await view_store.select(set(features))
                    .all(limit=int(limit))
                    .log_each_job()
                    .to_polars()
                )
                data = data.collect().to_pandas()
            end_time = datetime.now()

            st.success(f"Loaded {len(data)} rows in {end_time - start_time}")
            st.dataframe(data)

    with between_data:
        st.write("Load all existing data between two dates.")
        start_date = st.date_input("Start Date", key="load_start_date")
        end_date = st.date_input("End Date", key="load_end_date")

        if not (start_date and end_date):
            return

        start_date = datetime(start_date.year, start_date.month, start_date.day)
        end_date = datetime(end_date.year, end_date.month, end_date.day)

        if st.button("Load data", key="load_between_dates"):
            start_time = datetime.now()
            with st.spinner("Loading..."):
                data = await view_store.between_dates(start_date, end_date).to_pandas()
            end_time = datetime.now()

            st.success(f"Loaded {len(data)} rows in {end_time - start_time}")
            st.dataframe(data)


async def view_freshness(store: FeatureStore, view_name: str, key: str = "load_freshness"):
    view_store = store.feature_view(view_name)
    view = view_store.view

    st.write("Load and understand how fresh the data is.")

    if await use_data_managment_features():
        st.warning(
            "Data managment features are not enabled. Please enable them to use this feature.",
        )
        return

    if not view.event_timestamp:
        st.write(
            "An event timestamp is needed to compute the freshness. It can be set with the following code:",
        )
        st.code(
            """from aligned import EventTimestamp, feature_view

@feature_view(...)
class MyFeatureView:
    my_event_timestamp = EventTimestamp()
        """,
        )

        return

    source = view.materialized_source or view.source

    st.subheader("Data freshness")

    st.write(f"Data source is of type: `{type(source).__name__}`.")
    st.write(f"Basing freshness on `{view.event_timestamp.name}`")

    if st.button("Load Data Freshness", key=key):
        try:
            with st.spinner("Loading..."):
                freshness = await view_store.freshness()

            if freshness is None:
                st.write("No data found")
            else:
                delta = datetime.now() - freshness
                st.metric("Valid features up to", freshness.strftime("%Y-%m-%d %H:%M:%S"))
                st.metric("Updated ... ago", str(delta))
        except Exception as e:
            st.error(e)


def feature_location(location: FeatureLocation, col: DetalGenerator | None = None):
    col = col or st

    with col.container(border=True):
        left, right = col.columns(2)

        left.text(location.name)
        right.caption(location.location)


def show_mermiad_graph(
    graph_name: str,
    nodes: set[GraphNode],
    edges: set[GraphEdge],
    default_height: int = 250,
    graph_type: str = "LR",
    col: DeltaGenerator | None = None,
) -> None:
    col = col or st.columns(1)[0]

    col.subheader(graph_name)
    height_input = col.number_input("Height", value=default_height, key=graph_name)

    types = ["LR", "TB"]

    selected_type = col.selectbox(
        "Graph Type",
        types,
        index=types.index(graph_type),
        key=f"{graph_name}_type",
    )
    mermaid_graph = mermiad_graph(nodes, edges, graph_type=selected_type or graph_type)

    col._html(
        f"""
    <pre class="mermaid">
    {mermaid_graph}
    </pre>

    <script type="module">
      import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
      mermaid.initialize({{ startOnLoad: true }});
    </script>""",
        scrolling=True,
        height=int(height_input),
    )


def feature_view_dependencies(store: FeatureStore, view_name: str, col: DeltaGenerator) -> None:
    view_store = store.feature_view(view_name)
    view = view_store.view

    all_dependencies = view.source.depends_on()
    mermaid_edges: set[GraphEdge] = {
        GraphEdge(dep.identifier, f"feature_view:{view_name}") for dep in all_dependencies
    }

    if not all_dependencies:
        st.write(
            "The view has no dependencies. Therefore it is most likely a staging / source view.",
        )
        return

    col.subheader("One level dependencies")
    col.write("This following dependencies are the data that are needed to compute the features.")
    for dep_loc in view.source.depends_on():
        feature_location(dep_loc, col)

        if dep_loc.location == "feature_view":
            sub_deps = store.feature_view(dep_loc.name).view.source.depends_on()
            all_dependencies.update(sub_deps)

            for sub_dep in sub_deps:
                mermaid_edges.add(
                    GraphEdge(
                        sub_dep.identifier,
                        dep_loc.identifier,
                    ),
                )

    for dep_loc in all_dependencies.copy():
        if dep_loc.location == "feature_view":
            sub_deps = store.feature_view(dep_loc.name).view.source.depends_on()
            all_dependencies.update(sub_deps)

            for sub_dep in sub_deps:
                mermaid_edges.add(
                    GraphEdge(
                        sub_dep.identifier,
                        dep_loc.identifier,
                    ),
                )

    if len(all_dependencies) != len(view.source.depends_on()):
        st.subheader("All dependencies")
        st.write("The following dependencies are the end-to-end dependencies needed.")

        for dep_loc in all_dependencies:
            feature_location(dep_loc)

    nodes = {GraphNode(dep.name, dep.identifier, dep.location) for dep in all_dependencies}
    nodes.add(GraphNode(view_name, f"feature_view:{view_name}", "feature_view"))

    show_mermiad_graph("Source dependency graph", nodes, mermaid_edges)


async def update_materialized_source_for_view(store: FeatureStore, view_name: str) -> None:
    view_store = store.feature_view(view_name)
    view = view_store.view

    st.write("Update the materialized source with new data.")

    if await use_data_managment_features():
        st.warning(
            "Data managment features are not enabled. Please enable them to use this feature.",
        )
        return

    if not view.materialized_source:
        st.write(
            "No materialized source is set. Therefore it is most likely a staging / source view.",
        )
        return

    if not isinstance(view.materialized_source, WritableFeatureSource | DataFileReference):
        st.write(
            "The materialized source is not writable. Therefore it is most likely a staging / source view.",
        )
        return

    depends_on_view = view.source.depends_on()
    if depends_on_view:
        st.write("When updating the materialzed source will it load data from:")

        for dep in depends_on_view:
            feature_location(dep)

    all_data, between_data, incremental_data = st.tabs(
        ["Update All Data", "Update Between Dates", "Incremental Update"],
    )

    with all_data:
        st.write("Load all existing data and overwrite it into the materialized source.")
        if st.button("Update Materialized Source", key="update_all_materialized"):
            start_time = datetime.now()
            with st.spinner("Materializing..."):
                (
                    await view_store.using_source(view.source)
                    .all()
                    .write_to_source(view.materialized_source)
                )
            end_time = datetime.now()

            st.success(f"Materialization done {end_time - start_time}")

    with between_data:
        st.write(
            "Load all existing data between two dates, and upsert it into the materialized source.",
        )
        start_date = st.date_input("Start Date")
        end_date = st.date_input("End Date")

        if not (start_date and end_date):
            return

        start_date = datetime(start_date.year, start_date.month, start_date.day)
        end_date = datetime(end_date.year, end_date.month, end_date.day)

        if st.button("Update Materialized Source", key="update_between_materialized"):
            start_time = datetime.now()
            with st.spinner("Materializing..."):
                await view_store.upsert(
                    view_store.using_source(view.source).between_dates(start_date, end_date),
                )
            end_time = datetime.now()

            st.success(f"Materialization done {end_time - start_time}")

    with incremental_data:
        st.write(
            "Load all data since the last materialization, and upsert it into the materialized source.",
        )

        if st.button("Update Materialized Source", key="incremental_materialized"):
            start_date = await view_store.freshness()
            end_date = datetime.now()

            if not start_date:
                st.write("No freshness found. Loading all data.")
                start_time = datetime.now()
                with st.spinner("Materializing..."):
                    (
                        await view_store.using_source(view.source)
                        .all()
                        .write_to_source(view.materialized_source)
                    )
                end_time = datetime.now()
            else:
                formatted_start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
                st.write(
                    f"Found freshness '{formatted_start_date}'. Loading data from then until now.",
                )
                start_time = datetime.now()
                with st.spinner("Materializing..."):
                    await view_store.upsert(
                        view_store.using_source(view.source).between_dates(start_date, end_date),
                    )
                end_time = datetime.now()

            st.success(f"Materialization done {end_time - start_time}")


async def show_feature_view(store: FeatureStore, view_name: str):
    view_store = store.feature_view(view_name)
    view = view_store.view

    st.title(view_name)
    main_col, side_col = st.columns([2, 1])

    with side_col:
        st.subheader("Metadata")
        if view.tags:
            st.write(view.tags)

        if view.description:
            st.write(view.description)

        if view.contacts:
            st.subheader("Contacts")
            for contact in view.contacts:
                st.write(contact)

    with main_col:
        with st.expander("Feature Content", expanded=True):
            st.subheader("Entities")
            st.write(
                "To uniquely identify a row in the view, you need to provide the following entities",
            )
            for entity in view.entities:
                feature_cell(entity, st)

            if view.event_timestamp:
                st.subheader("Event Timestamp")
                st.write(
                    "The timestamp describing when the feature was valid from. This can be set to travel back in time. This will also be used to compute the freshness of a view.",
                )
                feature_cell(view.event_timestamp, st)

            if view.features:
                st.subheader("Features")
                for feature in view.features:
                    feature_cell(feature, st)

            if view.derived_features:
                st.subheader("Derived Features")
                st.write("These features are derived (aka. computed) from other features.")
                for feature in view.derived_features:
                    feature_cell(feature, st)

            if view.aggregated_features:
                st.subheader("Aggregated Features")
                for feature in view.aggregated_features:
                    feature_cell(feature.derived_feature, st)

    with side_col, st.expander("Source Dependencies"):
        feature_view_dependencies(store, view_name, st)

    with side_col, st.expander("Feature Dependencies"):
        nodes, edges = inter_feature_view_nodes(view)

        show_mermiad_graph("Feature dependency graph", nodes, edges, 500)

    with side_col, st.expander("Sources"):
        if view.materialized_source:
            st.subheader("Materialized Source")
            st.write("This is the default source to use, but may contain out-dated data.")
            st.write(view.materialized_source)

        st.subheader("Source")
        st.write(view.source)

    st.subheader("Data Managment")
    with st.container(border=True):
        data_freshness, update_source, load_data, explore_data, transform_features = st.tabs(
            [
                "Data Freshness",
                "Materialize Source",
                "Load Data",
                "Explore Data",
                "Transform Features",
            ],
        )
        with data_freshness:
            await view_freshness(store, view_name)

        with update_source:
            await update_materialized_source_for_view(store, view_name)

        with load_data:
            await manually_show_features(store, view_name)

        with explore_data:
            await load_feature_view_data(store, view_name)

        with transform_features:
            await test_transform_for(view_store)


# async def predict_on_model(model_register: ModelRegister):
#
#     with st.spinner("Loading available models..."):
#
#
#     if not selected_model:
#
#
#     with st.spinner("Loading model..."):
#
#     with st.spinner("Loading feature store"):
#
#     if not isinstance(model, ModelWrapper):
#
#     if model.model_contract not in store.models:
#
#
#     if model.contract_version:
#
#
#     for entity in input_request:
#
#     if predict_view.model_version_column:
#
#
#     for feature in pred_result.features.union(pred_result.entities):
#
#
#     if len(stored_features) != 1:
#
#
#         "Limit Entities": select_entities_from_view(store, input_request),
#         "Entity Range": select_entities_range(store, input_request),
#
#
#     if not entity_method:
#
#
#     if entities is None:
#
#     await predict_on_model_entities(
#         entities,
#         model,
#         model_store,
#         predict_view,
#         stored_features,
#         selected_model
#
#
# async def predict_on_model_entities(
#     entities: pl.DataFrame,
#     model: ModelWrapper,
#     model_store: ModelFeatureStore,
#     predict_view: PredictionsView,
#     stored_features: set[Feature],
# ):
#
#     #
#     with st.spinner("Loading features"):
#             entities,
#         ).drop_invalid(PanderaValidator())
#
#
#
#     with st.spinner("Predicting..."):
#
#
#     with st.spinner("Setting features"):
#         if predict_view.model_version_column:
#
#         if stored_et_column and stored_et_column.name not in with_pred.columns:
#
#
#
#     if not model_store.model.predictions_view:
#
#     if predict_view.model_version_column:
#         if st.button("Insert Predictions"):
#             with st.spinner("Storing..."):
#
#     if st.button("Upsert Predictions"):
#         with st.spinner("Storing..."):


async def select_entities_range(
    store: FeatureStore,
    needed_entities: set[Feature],
) -> pl.DataFrame | None:
    st.write("Select a view containing all the entities, and then select a limit.")

    with st.form("Select Entities Range"):
        view_name = st.selectbox("Select View", store.feature_views.keys(), index=None)

        st.subheader("Select limit")
        start_date = st.date_input("Start Date", key="select_start_date")
        end_date = st.date_input("Start Date", key="select_end_date")

        st.form_submit_button()

    if start_date > end_date:
        st.info("Start date is after end date. Swapping dates.")
        temp_date = start_date
        end_date = start_date
        start_date = temp_date

    if not view_name:
        st.warning("Need to select a view.")
        return

    view_store = store.feature_view(view_name)

    entity_columns = {ent.name for ent in needed_entities}
    all_returned_columns = set(view_store.view.request_all.request_result.all_returned_columns)
    missing_columns = {ent.name for ent in needed_entities if ent.name not in all_returned_columns}

    if missing_columns:
        st.warning(f"The view is missing the following columns: {missing_columns}")
        return

    with st.spinner("Loading..."):
        entities = (
            (await view_store.between_dates(start_date, end_date).to_polars())
            .select(entity_columns)
            .collect()
        )

    return entities


async def select_entities_from_view(
    store: FeatureStore,
    needed_entities: set[Feature],
) -> pl.DataFrame | None:
    st.write("Select a view containing all the entities, and then select a limit.")

    view_name = st.selectbox("Select View", store.feature_views.keys(), index=None)

    if not view_name:
        st.warning("Need to select a view.")
        return

    view_store = store.feature_view(view_name)

    entity_columns = {ent.name for ent in needed_entities}
    all_returned_columns = set(view_store.view.request_all.request_result.all_returned_columns)
    missing_columns = {ent.name for ent in needed_entities if ent.name not in all_returned_columns}

    if missing_columns:
        st.warning(f"The view is missing the following columns: {missing_columns}")
        return

    with st.form("Select Entities"):
        st.subheader("Select limit")
        limit = st.number_input("Number of entities", value=100)
        st.form_submit_button()

    if not limit:
        st.warning("Need to select a limit.")
        return

    with st.spinner("Loading..."):
        entities = (
            (await view_store.all(limit=int(limit)).to_polars()).select(entity_columns).collect()
        )

    return entities


async def data_catalog(store: FeatureStore, definition: RepoDefinition | None = None):
    st.sidebar.title("Data Catalog")

    number_of_views = len(store.feature_views)
    number_of_models = len(store.models)

    selection = st.sidebar.selectbox(
        "View Models or Features",
        ["Model Contracts", "Features", "Models"],
        index=None,
    )

    if selection == "Model Contracts":
        model_name = st.sidebar.selectbox("Select Model Contract", store.models.keys(), index=None)

        if not model_name:
            model_contracts_overview(store)
        else:
            await show_model_contract(store, model_name)
    elif selection == "Features":
        view_name = st.sidebar.selectbox("Select View", store.feature_views.keys(), index=None)

        if not view_name:
            feature_view_overview(store)
        else:
            await show_feature_view(store, view_name)
    elif selection == "Models":
        pass
    else:
        st.subheader("Overview")

        left, right = st.columns(2)
        left.metric("Model Contracts", number_of_models)
        right.metric("Feature Views", number_of_views)

        nodes, edges = overview_graph(store)
        show_mermiad_graph("Overview graph", nodes, edges, default_height=500)

        await generate_schema(store)

    # if definition:
    #
    #     if definition.metadata.github_url:


async def generate_schema(store: FeatureStore):
    from aligned import FileSource, PostgreSQLConfig

    psql_config = PostgreSQLConfig.localhost("localhost")

    sources = [
        ("Parquet", FileSource.parquet_at),
        ("Csv", FileSource.csv_at),
        ("Delta File", FileSource.delta_at),
        ("Psql Table", psql_config.table),
    ]

    with st.form("Select Source"):
        st.subheader("Select source")
        source_type = st.selectbox("Source Type", [source[0] for source in sources], index=None)
        st.form_submit_button()

    if not source_type:
        st.warning("Need to select a source type.")
        return

    selected_source = [source for source in sources if source[0] == source_type][0]
    function = selected_source[1]

    annotations = function.__annotations__.copy()
    defaults = dict(
        zip(reversed(function.__code__.co_varnames), reversed(function.__defaults__ or [])),
    )

    values = {}

    with st.form("Config Source"):
        for name, annotation in annotations.items():
            if name == "return":
                continue

            removed_optional = annotation
            if " | None" in annotation:
                removed_optional = annotation.replace(" | None", "")

            if removed_optional == "datetime":
                value = st.date_input(name)
            elif removed_optional == "str":
                value = st.text_input(name)
            elif removed_optional == "int":
                value = st.number_input(name)
            elif removed_optional == "bool":
                value = st.checkbox(name)
            elif removed_optional.startswith("dict"):
                st.caption(name)
                df = st.data_editor(
                    pd.DataFrame(columns=["key", "value"]),
                    num_rows="dynamic",
                    key=name,
                )
                value = dict(zip(df["key"].tolist(), df["value"].tolist()))
            else:
                value = None

            values[name] = value

        st.form_submit_button()

    for name, value in values.items():
        if (not value) and (name not in defaults):
            st.warning(f"Need to set {name}")
            return
        elif not value:
            values[name] = defaults.get(name)

    if not values:
        return

    source = function(**values)

    with st.form("Load Schema"):
        view_name = st.text_input("View Name")
        st.form_submit_button("Load Schama")

    if not view_name:
        st.warning("Need to set view name.")
        return

    code = await source.feature_view_code(view_name)
    st.code(code)


@dataclass
class NotSupportedTransformation(Transformation):
    dtype: FeatureType
    name = "Not Supported"

    async def transform_pandas(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series("Not Supporting Custom Transformations in this environment.")

    async def transform_polars(
        self,
        df: pl.LazyFrame,
        alias: str,
    ) -> pl.LazyFrame | pl.Expr | pl.Expr:
        return pl.lit("Custom Transformations are not supported in this environment.")

    @classmethod
    def from_dict(cls, d, **kwargs):
        return NotSupportedTransformation(FeatureType.string())


async def test_view():
    if not load_dotenv():
        st.warning("No .env file found.")

    st.set_page_config(page_title="Data Catalog", page_icon=":books:", layout="wide")

    st.header("Data Catalog")

    store = recommendation_feature_contracts()

    await data_catalog(store, store.repo_definition())


if __name__ == "__main__":
    asyncio.run(test_view())
