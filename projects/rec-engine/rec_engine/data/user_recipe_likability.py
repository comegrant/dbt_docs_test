from aligned import EventTimestamp, Int32, String, model_contract
from rec_engine.contacts import Contacts
from rec_engine.data.recipe import HistoricalRecipeOrders, RecipeIngredient, RecipeTaxonomies
from rec_engine.sources import model_preds

ingredient = RecipeIngredient()
recipes_taxonomies = RecipeTaxonomies()
orders = HistoricalRecipeOrders()


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

    score = orders.rating.as_regression_label()
