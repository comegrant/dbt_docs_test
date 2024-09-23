from aligned import (
    Bool,
    CustomMethodDataSource,
    EventTimestamp,
    FeatureStore,
    Float,
    Int32,
    List,
    String,
    Timestamp,
    feature_view,
)
from aligned.schemas.date_formatter import DateFormatter
from data_contracts.mealkits import OneSubMealkits
from data_contracts.preselector.basket_features import (
    ImportanceVector,
    PredefinedVectors,
    TargetVectors,
)
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek, MenuWeekRecipeNormalization, PreselectorYearWeekMenu
from data_contracts.recipe import NormalizedRecipeFeatures, RecipeCost, RecipeMainIngredientCategory, RecipePreferences
from data_contracts.recommendations.recommendations import PartitionedRecommendations
from data_contracts.sources import data_science_data_lake

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
            RecipePreferences.location,
            CostOfFoodPerMenuWeek.location,
            RecipeMainIngredientCategory.location,
            MenuWeekRecipeNormalization.location,
            PartitionedRecommendations.location,
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
)
class Preselector:
    agreement_id = Int32().lower_bound(1).as_entity()
    year = Int32().lower_bound(2024).upper_bound(2050).as_entity()
    week = Int32().upper_bound(52).lower_bound(1).as_entity()

    portion_size = Int32().lower_bound(1).upper_bound(6)
    company_id = String().accepted_values(
        [
            "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
            "8A613C15-35E4-471F-91CC-972F933331D7",
            "6A2D0B60-84D6-4830-9945-58D518D27AC2",
            "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
        ]
    )

    main_recipe_ids = List(Int32().lower_bound(1000).upper_bound(9999))
    variation_ids = List(String())
    generated_at = EventTimestamp()
    model_version = String()
    "The git hash of the program"


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
