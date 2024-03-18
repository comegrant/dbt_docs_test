from aligned import EventTimestamp, Int32, String, model_contract
from data_contracts.recommendations.recipe import HistoricalRecipeOrders, RecipeIngredient, RecipeTaxonomies
from data_contracts.sources import model_preds
from project_owners.owner import Owner

ingredient = RecipeIngredient()
recipes_taxonomies = RecipeTaxonomies()
orders = HistoricalRecipeOrders()


@model_contract(
    name="user_recipe_likability",
    description="The score of a recipe per user.",
    contacts=[
        Owner.niladri().markdown(),
        Owner.jose().markdown(),
        Owner.matsmoll().markdown(),
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
