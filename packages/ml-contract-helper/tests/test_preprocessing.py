from contextlib import suppress

import pandas as pd
import polars as pl
import pytest
from aligned import Bool, ContractStore, Float32, Int32, String, UInt32, data_contract, model_contract
from aligned.sources.random_source import RandomDataSource
from ml_contract_helper.preprocessing import preprocesser_from_contract, preprocesser_from_features
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler


@data_contract(source=RandomDataSource(fill_mode="random_samples"))
class SomeContract:
    recipe_id = Int32().as_entity()

    recipe_difficulty_level_id = Int32().is_nominal()
    recipe_main_ingredient_id = Int32().is_nominal()

    cooking_time_from = String().accepted_values(["15", "20", "25"]).is_ordinal()
    cooking_time_mean = Float32()
    cooking_time_to = Int32().lower_bound(20).upper_bound(40).is_interval()

    some_column_value = Bool()

    number_of_ingredients = Int32().lower_bound(0).is_ratio()
    number_of_recipe_steps = Int32().lower_bound(0).is_ratio()
    number_of_taxonomies = UInt32()


@model_contract(input_features=[SomeContract])
class SomeModelContract:
    recipe_id = Int32().as_entity()

    label = SomeContract().cooking_time_from.as_classification_label()


def assert_pipeline_contains(transformation: type, pipeline: Pipeline, message: str | None = None) -> None:
    try:
        component = next(tran for tran in pipeline.steps if isinstance(tran[1], transformation))
        assert component is not None
    except StopIteration as error:
        raise AssertionError(
            message or f"Expected at least one '{transformation}' but got none in {pipeline.steps}"
        ) from error


def find_transformation(pipeline: ColumnTransformer, feature_name: str) -> Pipeline | None:
    with suppress(StopIteration):
        return next(tran[1] for tran in pipeline.transformers if tran[1] != "passthrough" and feature_name in tran[2])


@pytest.mark.asyncio
async def test_sklearn_pipeline() -> None:
    schema = SomeContract()
    contract = SomeContract.compile()

    pipeline = preprocesser_from_features(features=list(contract.features))
    without_dtype = preprocesser_from_features(features=list(contract.features), feature_pipeline=lambda x: None)

    ordinal = find_transformation(pipeline, schema.cooking_time_from.name)
    assert ordinal is not None
    assert_pipeline_contains(OrdinalEncoder, ordinal)

    nominal = find_transformation(pipeline, schema.recipe_difficulty_level_id.name)
    assert nominal is not None
    assert_pipeline_contains(OneHotEncoder, nominal)

    uint_test = find_transformation(pipeline, schema.number_of_taxonomies.name)
    assert uint_test is not None
    assert_pipeline_contains(StandardScaler, uint_test)

    uint_test = find_transformation(without_dtype, schema.number_of_taxonomies.name)
    assert uint_test is None, "Should not add any transformations to the column"

    ratio = find_transformation(pipeline, schema.number_of_taxonomies.name)
    assert ratio is not None
    assert_pipeline_contains(StandardScaler, ratio)

    float_test = find_transformation(pipeline, schema.number_of_taxonomies.name)
    assert float_test is not None
    assert_pipeline_contains(StandardScaler, float_test)

    df = await SomeContract.query().all(limit=200).to_polars()

    pipeline.fit(df.to_pandas())

    more_df = await SomeContract.query().all(limit=200).to_polars()

    features = pipeline.transform(more_df)
    processed_df = pd.DataFrame(features)

    assert len(processed_df.columns) > len(more_df.columns)


@pytest.mark.asyncio
async def test_sklearn_pipeline_from_model_contract() -> None:
    store = ContractStore.empty()

    store.add(SomeContract)
    store.add(SomeModelContract)

    schema = SomeContract()

    pipeline = await preprocesser_from_contract(SomeModelContract, store)

    label = find_transformation(pipeline, schema.cooking_time_from.name)
    assert label is None, "The label / target should not be part of the transformation."

    df = await SomeContract.query().all(limit=200).to_polars()
    pipeline.fit(df.to_pandas())

    more_df = await SomeContract.query().all(limit=200).to_polars()

    features = pipeline.transform(more_df)
    processed_df = pd.DataFrame(features)

    assert len(processed_df.columns) > len(more_df.columns)


@pytest.mark.asyncio
async def test_remove_non_needed_features() -> None:
    store = ContractStore.empty()

    store.add(SomeContract)
    store.add(SomeModelContract)

    pipeline = await preprocesser_from_contract(SomeModelContract, store)

    df = await SomeContract.query().all(limit=200).to_polars()
    pipeline.fit(df)

    more_df = await SomeContract.query().all(limit=200).to_polars()

    features = pipeline.transform(more_df)
    features_df = pd.DataFrame(features)

    with_too_many = more_df.with_columns(additional_column=pl.lit("Some Value"), other=pl.lit("Some Value"))
    additional = pipeline.transform(with_too_many)
    with_too_many_df = pd.DataFrame(additional)

    assert features_df.columns.to_list() == with_too_many_df.columns.to_list()

    with_too_few = more_df.drop(
        SomeContract().number_of_ingredients.name,
        SomeContract().cooking_time_to.name,
    )
    with pytest.raises(ValueError):  # noqa: PT011
        _ = pipeline.transform(with_too_few)
