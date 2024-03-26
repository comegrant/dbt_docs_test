from aligned import EventTimestamp, Int32, String, model_contract
from aligned.schemas.date_formatter import DateFormatter
from data_contracts.recommendations.recipe import RecipeTaxonomies
from data_contracts.sources import recommendations_dir
from project_owners.owner import Owner

recipes_taxonomies = RecipeTaxonomies()


@model_contract(
    name="recipe_cluster",
    description="The cluster a recipe contains.",
    contacts=[
        Owner.niladri().markdown(),
        Owner.jose().markdown(),
        Owner.matsmoll().markdown(),
    ],
    features=[recipes_taxonomies.recipe_taxonomies],
    prediction_source=recommendations_dir.delta_at(
        "recipe_cluster_preds",
        date_formatter=DateFormatter.unix_timestamp(),
    ),
)
class RecipeCluster:
    recipe_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    predicted_at = EventTimestamp()
    model_version = String().as_model_version()

    cluster = Int32().is_required()
