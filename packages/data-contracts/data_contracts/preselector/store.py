from aligned import (
    Bool,
    CustomMethodDataSource,
    EventTimestamp,
    FeatureStore,
    Float,
    Int32,
    Json,
    List,
    String,
    Timestamp,
    feature_view,
)
from aligned.schemas.date_formatter import DateFormatter
from data_contracts.mealkits import OneSubMealkits
from data_contracts.orders import WeeksSinceRecipe
from data_contracts.preselector.basket_features import (
    ImportanceVector,
    PredefinedVectors,
    TargetVectors,
)
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek, MenuWeekRecipeNormalization, PreselectorYearWeekMenu
from data_contracts.recipe import (
    NormalizedRecipeFeatures,
    RecipeCost,
    RecipeEmbedding,
    RecipeMainIngredientCategory,
    RecipeNegativePreferences,
    RecipePreferences,
)
from data_contracts.recommendations.recommendations import RecommendatedDish
from data_contracts.sources import data_science_data_lake, databricks_catalog, redis_cluster

preselector_ab_test_dir = data_science_data_lake.directory("preselector/ab-test")


@feature_view(
    name="preselector_test_choice",
    source=preselector_ab_test_dir.delta_at("preselector_test_result_v3.delta"),
)
class PreselectorTestChoice:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    preselector_version = String().is_optional().fill_na("rulebased_v1")

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
    has_order_history = Bool().is_optional()

    created_at = Timestamp()
    updated_at = Timestamp()

    total_cost_of_food = Float().is_optional()
    concept_revenue = Float().is_optional()

    description = String().is_optional()


@feature_view(
    name="preselector_output",
    source=CustomMethodDataSource.from_methods(
        depends_on_sources={
            OneSubMealkits.location,
            RecipeEmbedding.location,
            WeeksSinceRecipe.location,
            RecommendatedDish.location,
            CostOfFoodPerMenuWeek.location,
            RecipeNegativePreferences.location,
            RecipeMainIngredientCategory.location,
            MenuWeekRecipeNormalization.location,
            NormalizedRecipeFeatures.location,
            PreselectorYearWeekMenu.location,
            PredefinedVectors.location,
            ImportanceVector.location,
            TargetVectors.location,
            RecipeCost.location,
        }
    ),
    materialized_source=data_science_data_lake.directory(
        "preselector/latest"
    ).partitioned_parquet_at(
        directory="output",
        partition_keys=["company_id", "year", "week"],
        date_formatter=DateFormatter.unix_timestamp(time_unit="us", time_zone="UTC"),
    ),
    stream_source=redis_cluster.stream("preselector-output")
)
class Preselector:
    agreement_id = Int32().lower_bound(1).as_entity()
    year = Int32().lower_bound(2024).upper_bound(2050).as_entity()
    week = Int32().upper_bound(53).lower_bound(1).as_entity()

    portion_size = Int32().lower_bound(1).upper_bound(6)
    company_id = String().accepted_values(
        [
            "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
            "8A613C15-35E4-471F-91CC-972F933331D7",
            "6A2D0B60-84D6-4830-9945-58D518D27AC2",
            "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
        ]
    ).is_optional()

    error_vector = Json().is_optional()
    main_recipe_ids = List(Int32().lower_bound(1000))
    variation_ids = List(String())
    generated_at = EventTimestamp()
    compliancy = Int32().lower_bound(0).upper_bound(4)
    concept_preference_ids = List(String())
    taste_preferences = List(Json())
    model_version = String()
    "The git hash of the program"


@feature_view(
    name="preselector_successful_live_output",
    source=databricks_catalog().schema("mloutputs").table("preselector_successful_realtime_output")

)
class SuccessfulPreselectorOutput:
    billing_agreement_id = Int32().lower_bound(1).as_entity()
    menu_year = Int32().lower_bound(2024).upper_bound(2050).as_entity()
    menu_week = Int32().upper_bound(53).lower_bound(1).as_entity()

    portion_size = Int32().is_optional()
    number_of_recipes = Int32().is_optional()
    company_id = String().accepted_values(
        [
            "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
            "8A613C15-35E4-471F-91CC-972F933331D7",
            "6A2D0B60-84D6-4830-9945-58D518D27AC2",
            "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
        ]
    ).is_optional()

    target_cost_of_food_per_recipe = Float()
    error_vector = Json().is_optional()
    main_recipe_ids = List(Int32())
    variation_ids = List(String())
    generated_at = EventTimestamp()
    compliancy = Int32().lower_bound(0).upper_bound(4)
    concept_preference_ids = List(String())
    taste_preferences = List(Json())
    model_version = String()
    has_data_processing_consent = Bool()
    override_deviation = Bool()


@feature_view(
    name="preselector_failed_realtime_output",
    source=databricks_catalog().schema("mloutputs").table("preselector_failed_realtime_output")
)
class FailedPreselectorOutput:
    error_message = Int32()
    error_code = Int32()
    request = String()
    "Contains the original request as a Json object"


def preselector_contracts() -> FeatureStore:
    store = FeatureStore.experimental()

    views = [
        PreselectorTestChoice,
        RecipePreferences,
        Preselector,
    ]

    for view in views:
        store.add_compiled_view(view.compile())

    return store
