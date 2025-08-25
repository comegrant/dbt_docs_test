from __future__ import annotations

import base64
import json
import logging
import zlib
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

import mlflow
from mlflow.models import ModelSignature
from mlflow.pyfunc import PyFuncModel

from model_registry.interface import ModelMetadata, ModelRef

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from aligned import ContractStore
    from aligned.feature_store import ConvertableToRetrievalJob
    from aligned.schemas.feature import FeatureReference


logger = logging.getLogger(__name__)


def decode_feature_refs(value: str) -> list[str] | None:
    if not value:
        return None

    with suppress(Exception):
        json_value = json.loads(value)
        assert isinstance(json_value, list)
        return json_value

    with suppress(Exception):
        decoded = base64.b85decode(value)
        list_value = zlib.decompress(decoded).decode()
        json_value = json.loads(list_value)
        assert isinstance(json_value, list)
        return json_value

    return None


def load_model(
    model_ref: ModelRef, feature_refs: list[str] | None = None, output_name: str | None = None
) -> tuple[PyFuncModel, list[str] | None, str]:
    model: PyFuncModel = mlflow.pyfunc.load_model(model_ref.model_uri)

    if not output_name:
        signature: ModelSignature = model.metadata.signature
        if signature is None:
            raise ValueError("Needs a signature in order to know what to name the output preds.")

        with suppress(Exception):
            output_name = str(signature.outputs.input_names()[0])

    assert output_name, "Unable to find an output name, so manually define it."

    if feature_refs is not None:
        return model, feature_refs, output_name

    if model.metadata.metadata is not None:
        feature_refs = decode_feature_refs(model.metadata.metadata.get(ModelMetadata.feature_reference, ""))
    else:
        mlflow_client = mlflow.MlflowClient()

        if model_ref.version_type == "alias":
            model_version = mlflow_client.get_model_version_by_alias(model_ref.name, model_ref.version)
        else:
            model_version = mlflow_client.get_model_version(model_ref.name, model_ref.version)

        feature_refs = decode_feature_refs(model_version.tags.get(ModelMetadata.feature_reference, ""))

    return model, feature_refs, output_name


@dataclass
class UnityCatalogColumn:
    schema: str
    table: str
    column: str

    @staticmethod
    def from_string(ref: str) -> UnityCatalogColumn:
        components = ref.split(".")

        assert len(components) == 3, (  # noqa: PLR2004
            f"Expected three components, representing sehcma, table and column but got {len(components)}"
        )
        return UnityCatalogColumn(schema=components[0], table=components[1], column=components[2])


def structure_feature_refs(refs: list[str]) -> list[UnityCatalogColumn] | list[FeatureReference]:
    decoder = UnityCatalogColumn.from_string

    if ":" in refs[0]:
        from aligned.schemas.feature import FeatureReference

        decoder = FeatureReference.from_string

    all_refs = []
    for ref in refs:
        val = decoder(ref)
        assert val is not None, f"Unable to decode '{ref}' when using '{decoder}'"
        all_refs.append(val)

    return all_refs


def add_features_to(entities: pd.DataFrame, table_path: str, columns: set[str]) -> pd.DataFrame:
    from catalog_connector import session_or_serverless

    entity_columns = entities.columns

    spark = session_or_serverless.spark()
    table = spark.table(table_path)

    schema = table.schema

    join_columns = set(schema.fieldNames()).intersection(entity_columns.to_list())

    if not join_columns:
        raise ValueError(
            f"Was unable to find any overlap between the entities and the feature table '{table_path}'.\n"
            f"Columns for the entities are: {entities.columns.to_list()} while the table has {schema.fieldNames()}"
        )

    all_columns = sorted(join_columns.union(columns))

    # Naive approache, but it gets the job done
    features = table.select(all_columns).toPandas()

    return entities.merge(features, how="left", on=list(join_columns))


def features_for_uc(refs: list[UnityCatalogColumn], entities: pd.DataFrame) -> pd.DataFrame:
    feature_order = [ref.column for ref in refs]
    table_lookups: dict[str, set[str]] = defaultdict(set)

    for ref in refs:
        table_path = f"{ref.schema}.{ref.table}"
        table_lookups[table_path].add(ref.column)

    features = entities.copy()
    for table, columns in table_lookups.items():
        height = features.shape[0]
        features = add_features_to(features, table, columns)
        assert height == features.shape[0], (
            f"There is not a 1:1 mapping between the entities and '{table}'. "
            "all sure you provide all needed ids to get a 1:1 mapping."
        )

    return features[feature_order]  # type: ignore


async def features_from_contracts(
    refs: list[FeatureReference], entities: ConvertableToRetrievalJob, store: ContractStore
) -> pl.DataFrame:
    return await store.features_for(entities, features=refs).to_polars()
