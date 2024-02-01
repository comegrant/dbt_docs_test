from aligned import EventTimestamp, Float, Int32, String, model_contract
from data_contracts.contacts import Contacts
from data_contracts.recommendations.recipe import RecipeIngredient, RecipeTaxonomies
from data_contracts.sources import model_preds

ingredient = RecipeIngredient()
recipes_taxonomies = RecipeTaxonomies()


@model_contract(
    name="user_recipe_likability",
    description="The score of a recipe per user.",
    contacts=[
        Contacts.niladri().markdown(),
        Contacts.jose().markdown(),
        Contacts.matsmoll().markdown(),
    ],
    features=[ingredient.all_ingredients, recipes_taxonomies.recipe_taxonomies],
    prediction_source=model_preds.parquet_at("user_recipe_likability.parquet"),
)
class UserRecipeLikability:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()

    predicted_at = EventTimestamp()
    model_version = String().as_model_version()

    score = Float().is_required()
