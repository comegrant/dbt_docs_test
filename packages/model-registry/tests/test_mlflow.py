from uuid import uuid4

import mlflow
import pandas as pd
import polars as pl
import pytest
from aligned.sources.random_source import RandomDataSource
from model_registry import ModelMetadata
from model_registry.mlflow import ModelRef
from model_registry.mlflow_infer import UnityCatalogColumn, load_model
from pytest_mock import MockerFixture


@pytest.mark.asyncio
async def test_mlflow(mocker: MockerFixture) -> None:
    from model_registry.mlflow import mlflow_registry

    alias = "champion"
    run_url = f"https://test.com/{uuid4()}"
    model_name = "test"
    example_input = pd.DataFrame({"a": [1, 2, 3], "b": [2, 3, 1]})

    mocker.patch("model_registry.mlflow_infer.add_features_to", return_value=example_input)

    def function(data: pd.DataFrame) -> pd.Series:
        return data.sum(axis=1, numeric_only=True)

    registry = mlflow_registry()

    await (
        registry.alias(alias)
        .training_run_url(run_url)
        .training_dataset(example_input)
        .feature_references(
            [
                "mlgold.attribute_scoring_recipes.cooking_time_from",
                "mlgold.attribute_scoring_recipes.cooking_time_to",
                "mlgold.attribute_scoring_recipes.cooking_time_mean",
                "mlgold.attribute_scoring_recipes.recipe_difficulty_level_id",
                "mlgold.attribute_scoring_recipes.recipe_main_ingredient_id",
                "mlgold.attribute_scoring_recipes.number_of_taxonomies",
                "mlgold.attribute_scoring_recipes.number_of_ingredients",
                "mlgold.attribute_scoring_recipes.number_of_recipe_steps",
                "mlgold.dishes_forecasting_recipe_ingredients.has_chicken_filet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_chicken",
                "mlgold.dishes_forecasting_recipe_ingredients.has_dry_pasta",
                "mlgold.dishes_forecasting_recipe_ingredients.has_fresh_pasta",
                "mlgold.dishes_forecasting_recipe_ingredients.has_white_fish_filet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_cod_fillet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_breaded_cod",
                "mlgold.dishes_forecasting_recipe_ingredients.has_salmon_filet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_seafood",
                "mlgold.dishes_forecasting_recipe_ingredients.has_pork_filet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_pork_cutlet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_trout_filet",
                "mlgold.dishes_forecasting_recipe_ingredients.has_parmasan",
                "mlgold.dishes_forecasting_recipe_ingredients.has_cheese",
                "mlgold.dishes_forecasting_recipe_ingredients.has_minced_meat",
                "mlgold.dishes_forecasting_recipe_ingredients.has_burger_patty",
                "mlgold.dishes_forecasting_recipe_ingredients.has_noodles",
                "mlgold.dishes_forecasting_recipe_ingredients.has_sausages",
                "mlgold.dishes_forecasting_recipe_ingredients.has_tortilla",
                "mlgold.dishes_forecasting_recipe_ingredients.has_pizza_crust",
                "mlgold.dishes_forecasting_recipe_ingredients.has_bacon",
                "mlgold.dishes_forecasting_recipe_ingredients.has_wok_sauce",
                "mlgold.dishes_forecasting_recipe_ingredients.has_asian_sauces",
                "mlgold.dishes_forecasting_recipe_ingredients.has_salsa",
                "mlgold.dishes_forecasting_recipe_ingredients.has_flat_bread",
                "mlgold.dishes_forecasting_recipe_ingredients.has_pita",
                "mlgold.dishes_forecasting_recipe_ingredients.has_whole_salad",
                "mlgold.dishes_forecasting_recipe_ingredients.has_shredded_vegetables",
                "mlgold.dishes_forecasting_recipe_ingredients.has_potato",
                "mlgold.dishes_forecasting_recipe_ingredients.has_peas",
                "mlgold.dishes_forecasting_recipe_ingredients.has_rice",
                "mlgold.dishes_forecasting_recipe_ingredients.has_nuts",
                "mlgold.dishes_forecasting_recipe_ingredients.has_beans",
                "mlgold.dishes_forecasting_recipe_ingredients.has_onion",
                "mlgold.dishes_forecasting_recipe_ingredients.has_citrus",
                "mlgold.dishes_forecasting_recipe_ingredients.has_sesame",
                "mlgold.dishes_forecasting_recipe_ingredients.has_herbs",
                "mlgold.dishes_forecasting_recipe_ingredients.has_fruit",
                "mlgold.dishes_forecasting_recipe_ingredients.has_cucumber",
                "mlgold.dishes_forecasting_recipe_ingredients.has_chili",
                "mlgold.dishes_forecasting_recipe_ingredients.has_pancake",
            ]
        )
        .register_as(model_name, function)  # type: ignore
    )

    latest_version = mlflow.MlflowClient().get_model_version_by_alias(model_name, alias)
    uri = f"models:/{model_name}/{latest_version.version}"
    model = mlflow.pyfunc.load_model(uri)

    metadata = model.metadata.metadata
    assert metadata is not None
    assert ModelMetadata.training_run_url in metadata
    assert metadata[ModelMetadata.training_run_url] == run_url

    preds = model.predict(example_input)
    assert preds.shape[0] == example_input.shape[0]

    _, refs, output = load_model(ModelRef(model_name, latest_version.version, "version"), output_name="output")
    assert refs
    assert output
    _, refs, output = load_model(ModelRef(model_name, alias, "alias"), output_name="output")
    assert refs
    assert output

    # Just to ignore the real refs, and select the same as the input frame.
    # This should still decode the features.
    # FIXME: Add a separate decode feature refs test
    mocker.patch(
        "model_registry.mlflow.structure_feature_refs",
        return_value=[
            UnityCatalogColumn("test", "test", "a"),
            UnityCatalogColumn("test", "test", "b"),
        ],
    )

    preds = await registry.infer_over(
        pd.DataFrame({"id": [11, 22, 33]}), ModelRef(model_name, alias, "alias"), output_name="preds"
    )
    assert not preds.empty


@pytest.mark.asyncio
async def test_mlflow_aligned_refs() -> None:
    from aligned import Int32, data_contract, model_contract
    from model_registry.mlflow import mlflow_registry

    alias = "champion"
    model_name = "test"
    example_input = pd.DataFrame({"a": [1, 2, 3], "b": [2, 3, 1]})
    example_input["a"] = example_input["a"].astype("int32")
    example_input["b"] = example_input["b"].astype("int32")

    @data_contract(source=RandomDataSource(fill_mode="random_samples"))
    class Input:
        id = Int32().as_entity()
        a = Int32()
        b = Int32()

        kind_of_label = Int32()

    @model_contract(input_features=[Input().a, Input().b])
    class SomeModel:
        some_label_name = Input().kind_of_label.as_classification_label()

    store = SomeModel.query([Input]).store

    def function(data: pd.DataFrame) -> pd.Series:
        return data.sum(axis=1, numeric_only=True)

    registry = mlflow_registry()
    await (
        registry.alias(alias)
        .training_dataset(example_input)
        .feature_references(store.contract(SomeModel).input_features())
        .signature(store.contract(SomeModel))
        .register_as(model_name, function)  # type: ignore
    )

    uri = f"{model_name}@{alias}"

    preds = await registry.infer_over(pl.DataFrame({"id": [11, 22, 33]}), uri, store=store)
    assert not preds.is_empty()
    assert "some_label_name" in preds.columns
    assert "id" in preds.columns
