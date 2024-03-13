from aligned import EventTimestamp, Int32, model_contract
from data_contracts.recommendations.recipe import (
    HistoricalRecipeOrders,
    RecipeIngredient,
    RecipeTaxonomies,
)
from data_contracts.sources import azure_dl_creds

from rec_engine.owner import contact

model_data_dir = azure_dl_creds.directory("models/rec-engine")


@model_contract(
    name="rec-engine",
    features=[
        RecipeTaxonomies().recipe_taxonomies,
        RecipeIngredient().all_ingredients,
    ],
    contacts=[contact.markdown()],
    description="",
    prediction_source=model_data_dir.directory("predictions").parquet_at(
        "preds.parquet",
    ),
    exposed_at_url="https://rec-engine:8501",  # This is the link in our docker compose setup
    dataset_store=model_data_dir.json_at("datasets.json"),
)
class YourModelContract:
    some_id = Int32().as_entity()

    predicted_at = EventTimestamp()

    predicted_rating = HistoricalRecipeOrders().rating.as_regression_label()
