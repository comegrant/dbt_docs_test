from aligned import EventTimestamp, Int32, model_contract
from data_contracts.recommendations.recipe import (
    HistoricalRecipeOrders,
    RecipeIngredient,
    RecipeTaxonomies,
)
from data_contracts.sources import azure_dl_creds
from project_owners.owner import Owner

model_data_dir = azure_dl_creds.directory("models/rec-engine")


@model_contract(
    name="rec-engine",
    input_features=[
        RecipeTaxonomies().recipe_taxonomies,
        RecipeIngredient().all_ingredients,
    ],
    contacts=[Owner.matsmoll().markdown()],
    output_source=model_data_dir.directory("predictions").parquet_at(
        "preds.parquet",
    ),
    exposed_at_url="https://rec-engine:8501",  # This is the link in our docker compose setup
    dataset_store=model_data_dir.json_at("datasets.json"),
)
class YourModelContract:
    some_id = Int32().as_entity()

    predicted_at = EventTimestamp()

    predicted_rating = HistoricalRecipeOrders().rating.as_recommendation_target()
