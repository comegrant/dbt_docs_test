from aligned import Bool, Float, Int32, String, Timestamp, feature_view
from aligned.compiler.feature_factory import List
from data_contracts.sources import data_science_data_lake

preselector_ab_test_dir = data_science_data_lake.directory("preselector/ab-test")


@feature_view(
    name="recipe_information",
    source=data_science_data_lake.delta_at("recipe_information.delta"),
)
class RecipeInformation:
    main_recipe_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    cooking_time_from = Int32()
    cooking_time_to = Int32()

    average_cooking_time = (cooking_time_from + cooking_time_to) / 2

    recipe_photo = String()
    recipe_name = String()
    taxonomie_names = String()

    taxonomies = taxonomie_names.transform_pandas(
        lambda x: x["taxonomie_names"].str.split(", ").apply(lambda x: list(set(x))),
        as_dtype=List(String()),
    )
    photo_url = recipe_photo.prepend("https://pimimages.azureedge.net/images/resized/")


@feature_view(
    name="preselector_test_choice",
    source=preselector_ab_test_dir.delta_at("preselector_test_result.delta"),
)
class PreselectorTestChoice:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    main_recipe_ids = List(Int32())
    number_of_recipes_to_change = Int32().is_optional()

    compared_main_recipe_ids = List(Int32())
    compared_number_of_recipes_to_change = Int32().is_optional()

    chosen_mealkit = String().accepted_values(["pre-selector", "chef-selection"])

    was_lower_cooking_time = Bool()
    was_more_variety = Bool()
    was_more_interesting = Bool()
    was_more_family_friendly = Bool()
    was_better_recipes = Bool()
    was_better_proteins = Bool()
    was_better_sides = Bool()
    was_better_images = Bool()
    was_fewer_unwanted_ingredients = Bool()
    had_recipes_last_week = Bool()

    created_at = Timestamp()
    updated_at = Timestamp()

    total_cost_of_food = Float().is_optional()
    concept_revenue = Float().is_optional()

    description = String().is_optional()
