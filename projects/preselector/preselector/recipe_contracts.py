from aligned import EventTimestamp, Int32, List, model_contract
from data_contracts.recommendations.recipe import RecipeCost, RecipeFeatures, RecipeNutrition
from project_owners.owner import Owner

recipe_features = RecipeFeatures()
recipe_nutrition = RecipeNutrition()
recipe_cost = RecipeCost()


@model_contract(
    name="preselector",
    features=[
        recipe_features.cooking_time_from,
        recipe_features.is_kids_friendly,
        recipe_features.is_family_friendly,
        recipe_nutrition.energy_kcal_100g,
        recipe_nutrition.fat_100g,
        recipe_nutrition.fat_saturated_100g,
        recipe_nutrition.protein_100g,
        recipe_nutrition.fruit_veg_fresh_100g,
        recipe_cost.price_category_level,
        recipe_cost.recipe_cost_whole_units,
        recipe_cost.is_premium,
        recipe_cost.is_cheep,
    ],
    contacts=[Owner.matsmoll().markdown()],
    description="Selects an optimal mealkit based on a few features by finding the combination that is closest to a target vector.",
)
class Preselector:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    predicted_at = EventTimestamp()

    selected_main_recipe_ids = List(Int32())
