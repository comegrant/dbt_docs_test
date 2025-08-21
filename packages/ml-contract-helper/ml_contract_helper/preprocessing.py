import logging
from collections import defaultdict
from contextlib import suppress
from typing import Callable

import numpy as np
from aligned import ContractStore
from aligned.feature_store import ModelContractWrapper
from aligned.schemas.constraints import InDomain
from aligned.schemas.feature import Feature, FeatureType, StaticFeatureTags
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

logger = logging.getLogger(__name__)


def preprocesser_from_features(
    features: list[Feature],
    pipeline_priorities: list[tuple[str, Pipeline | Callable[[Feature], Pipeline | None]]] | None = None,
    feature_pipeline: Callable[[Feature], Pipeline | None] | None = None,
) -> ColumnTransformer:
    def default_data_pipeline(feature: Feature) -> Pipeline | None:
        if feature.dtype.name.startswith("float"):
            return Pipeline(steps=[("impute", SimpleImputer(strategy="mean")), ("scaler", StandardScaler())])

        if feature.dtype.name.startswith("uint"):
            return Pipeline(steps=[("impute", SimpleImputer(strategy="median")), ("scaler", StandardScaler())])

        return None

    def ordinal_step(feature: Feature) -> Pipeline | None:
        encoder = None

        if feature.dtype == FeatureType.string() and feature.constraints:
            with suppress(StopIteration):
                order = next(con for con in feature.constraints if isinstance(con, InDomain))
                encoder = OrdinalEncoder(
                    categories=[order.values],  # type: ignore
                    handle_unknown="use_encoded_value",
                    unknown_value=np.nan,
                )

        return Pipeline(
            steps=[
                ("impute", SimpleImputer(strategy="most_frequent")),
                ("encode", encoder or OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan)),
            ]
        )

    if not pipeline_priorities:
        pipeline_priorities = [
            (
                StaticFeatureTags.is_nominal,
                Pipeline(
                    steps=[
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        # Using sparse_output=False as it fixes some issues when working with strings
                        ("encode", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
            ),
            (StaticFeatureTags.is_ordinal, ordinal_step),
            (
                StaticFeatureTags.is_interval,
                Pipeline(steps=[("impute", SimpleImputer(strategy="mean")), ("scaler", StandardScaler())]),
            ),
            (
                StaticFeatureTags.is_ratio,
                Pipeline(steps=[("impute", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]),
            ),
        ]

    if feature_pipeline is None:
        feature_pipeline = default_data_pipeline

    pipelines: dict[str, Pipeline] = {}
    preprocessing_steps: dict[str, list[str]] = defaultdict(list)

    for feature in features:
        if feature.tags is None:
            datatype_pipe = feature_pipeline(feature)

            if datatype_pipe is not None:
                preprocessing_steps[feature.dtype.name].append(feature.name)
                pipelines[feature.dtype.name] = datatype_pipe

            continue

        has_pipeline = False

        for tag, pipeline in pipeline_priorities:
            if tag in feature.tags:
                if not isinstance(pipeline, Pipeline):
                    pipe = pipeline(feature)
                else:
                    pipe = pipeline

                if not pipe:
                    break

                if pipelines.get(tag, pipe) == pipe:
                    preprocessing_steps[tag].append(feature.name)
                    pipelines[tag] = pipe
                else:
                    preprocessing_steps[feature.name].append(feature.name)
                    pipelines[feature.name] = pipe

                has_pipeline = True
                break

        if has_pipeline:
            continue

        datatype_pipe = feature_pipeline(feature)

        if datatype_pipe is not None:
            preprocessing_steps[feature.dtype.name].append(feature.name)
            pipelines[feature.dtype.name] = datatype_pipe

    return ColumnTransformer(
        [
            ("select", "passthrough", [feat.name for feat in features]),
            *[(tag, pipelines[tag], columns) for tag, columns in preprocessing_steps.items()],
        ],
        remainder="drop",
    )


async def preprocesser_from_contract(
    model_contract: ModelContractWrapper,
    store: ContractStore | None = None,
    pipeline_priorities: list[tuple[str, Pipeline | Callable[[Feature], Pipeline | None]]] | None = None,
    datatype_pipeline: Callable[[Feature], Pipeline | None] | None = None,
) -> ColumnTransformer:
    if store is None:
        from data_contracts.store import all_contracts

        logger.info("Loading the default store")
        store = await all_contracts()

    model_store = store.contract(model_contract)

    input_req = model_store.input_request().request_result
    label_names = [ref.name for ref in model_store.model.predictions_view.labels_estimates_refs()]
    input_features = [feat for feat in input_req.features if feat.name not in label_names]

    logger.info(f"Creating preprocessing from features named '{[feat.name for feat in input_features]}'")

    return preprocesser_from_features(
        features=input_features,
        pipeline_priorities=pipeline_priorities,
        feature_pipeline=datatype_pipeline,
    )
