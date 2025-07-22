from aligned import EventTimestamp, Int32, String, model_contract
from data_contracts.orders import HistoricalRecipeOrders
from data_contracts.recipe import (
    RecipeFeatures,
    RecipeIngredient,
    RecipeTaxonomies,
)
from data_contracts.sources import recommendations_dir
from project_owners.owner import Owner

ingredient = RecipeIngredient()
recipes_taxonomies = RecipeTaxonomies()
recipe_features = RecipeFeatures()
orders = HistoricalRecipeOrders()


@model_contract(
    name="user_recipe_likability",
    description="The score of a recipe per user.",
    contacts=[
        Owner.matsmoll().name,
    ],
    input_features=[
        ingredient.all_ingredients,
        recipes_taxonomies.recipe_taxonomies,
        recipe_features.is_low_cooking_time,
        recipe_features.is_medium_cooking_time,
        recipe_features.is_high_cooking_time,
    ],
    output_source=recommendations_dir.partitioned_parquet_at(
        "user_recipe_likability", partition_keys=["company_id", "year_week"]
    ),
)
class UserRecipeLikability:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()
    year_week = Int32()

    company_id = String()

    predicted_at = EventTimestamp()
    model_version = String().as_model_version()

    score = orders.rating.as_regression_label()
