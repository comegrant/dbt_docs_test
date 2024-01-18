from aligned import EventTimestamp, Int32, String, model_contract

from cheffelo_personalization.rec_engine.contacts import Contacts
from cheffelo_personalization.rec_engine.data.recipe import RecipeTaxonomies
from cheffelo_personalization.rec_engine.sources import model_preds

recipes_taxonomies = RecipeTaxonomies()


@model_contract(
    name="recipe_cluster",
    description="The cluster a recipe contains.",
    contacts=[
        Contacts.niladri().markdown(),
        Contacts.jose().markdown(),
        Contacts.matsmoll().markdown(),
    ],
    features=[recipes_taxonomies.recipe_taxonomies],
    prediction_source=model_preds.parquet_at("recipe_cluster.parquet"),
)
class RecipeCluster:
    recipe_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    predicted_at = EventTimestamp()
    model_version = String().as_model_version()

    cluster = Int32().is_required()
