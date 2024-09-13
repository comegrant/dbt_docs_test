from aligned import EventTimestamp, FileSource, Int32, List, model_contract
from data_contracts.recipe import RecipeCost, RecipeFeatures, RecipeNutrition
from project_owners.owner import Owner

recipe_features = RecipeFeatures()
recipe_nutrition = RecipeNutrition()
recipe_cost = RecipeCost()

cache_dir = FileSource.directory("data")


@model_contract(
    name="preselector",
    input_features=[
        recipe_cost.is_cheep,
    ],
    contacts=[Owner.matsmoll().markdown()],
    description=(
        "Selects an optimal mealkit based on a few features by "
        "finding the combination that is closest to a target vector."
    ),
)
class Preselector:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    predicted_at = EventTimestamp()

    selected_main_recipe_ids = List(Int32())
