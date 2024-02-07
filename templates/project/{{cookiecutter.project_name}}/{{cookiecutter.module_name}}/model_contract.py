from aligned import model_contract, FileSource, Int32, EventTimestamp, feature_view

from data_contracts.recommendations.recipe import RecipeTaxonomies, HistoricalRecipeOrders, RecipeIngredient
from data_contracts.sources import azure_dl_creds 

from {{cookiecutter.module_name}}.owner import contact

model_data_dir = azure_dl_creds.directory("models/{{cookiecutter.project_name}}")


@feature_view(
    name="my_custom_features",
    source=FileSource.parquet_at("features.parquet"),
    contacts=[
        contact.markdown()
    ]
)
class MyFeatures:
    recipe_id = Int32().as_entity()
    updated_at = EventTimestamp()

    score = Int32()
    transformation = score * 10


@model_contract(
    name="{{cookiecutter.project_name}}",
    features=[
        RecipeTaxonomies().recipe_taxonomies,
        RecipeIngredient().all_ingredients,
        MyFeatures().transformation
    ],
    contacts=[
        contact.markdown()
    ],
    description="",
    prediction_source=model_data_dir.directory("predictions").parquet_at("preds.parquet"),
    exposed_at_url="https://{{cookiecutter.project_name}}:8501", # This is the link in our docker compose setup
    dataset_store=model_data_dir.json_at("datasets.json"),
)
class YourModelContract:
    some_id = Int32().as_entity()

    predicted_at = EventTimestamp()

    predicted_rating = HistoricalRecipeOrders().rating.as_regression_label()
