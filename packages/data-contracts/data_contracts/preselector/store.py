from aligned import (
    Bool,
    CustomMethodDataSource,
    EventTimestamp,
    FeatureStore,
    Float,
    Int32,
    List,
    String,
    Struct,
    feature_view,
)
from aligned.schemas.date_formatter import DateFormatter
from data_contracts.attribute_scoring import AttributeScoring
from data_contracts.mealkits import OneSubMealkits
from data_contracts.orders import WeeksSinceRecipe
from data_contracts.preselector.basket_features import (
    ImportanceVector,
    PredefinedVectors,
    PreselectorErrorStructure,
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
from data_contracts.sources import data_science_data_lake, databricks_catalog
from pydantic import BaseModel


class PreselectorRecipeOutput(BaseModel):
    main_recipe_id: int
    variation_id: str
    compliancy: int


class AllergenPreference(BaseModel):
    preference_id: str
    is_allergy: bool


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
            AttributeScoring.location,
        }
    ),
    materialized_source=data_science_data_lake.directory("preselector/latest").partitioned_parquet_at(
        directory="output",
        partition_keys=["company_id", "year", "week"],
        date_formatter=DateFormatter.unix_timestamp(time_unit="us", time_zone="UTC"),
    ),
)
class Preselector:
    agreement_id = Int32().lower_bound(1).as_entity()
    year = Int32().lower_bound(2024).upper_bound(2050).as_entity()
    week = Int32().upper_bound(53).lower_bound(1).as_entity()

    portion_size = Int32()
    company_id = (
        String()
        .accepted_values(
            [
                "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
                "8A613C15-35E4-471F-91CC-972F933331D7",
                "6A2D0B60-84D6-4830-9945-58D518D27AC2",
                "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
            ]
        )
        .is_optional()
    )

    error_vector = Struct(PreselectorErrorStructure).is_optional()
    main_recipe_ids = List(Int32()).description("The selected main recipe ids")
    variation_ids = List(String()).description("The variation ids")
    generated_at = EventTimestamp()
    compliancy = Int32().lower_bound(0).upper_bound(4)
    concept_preference_ids = List(String()).description("The concept preference ids used to generate the output")

    weeks_since_selected = Struct().is_optional().description("Contains the main_recipe_id and the year week")

    recipes = List(Struct(PreselectorRecipeOutput)).is_optional().description("The outputted recipes")

    taste_preferences = List(Struct(AllergenPreference))

    model_version = String()
    "The git hash of the program"


@feature_view(
    source=data_science_data_lake.directory("preselector/latest").partitioned_parquet_at(
        "output-slim",
        partition_keys=["company_id", "year", "week"],
        date_formatter=DateFormatter.unix_timestamp(time_unit="us", time_zone="UTC"),
    )
)
class ForecastedMealkits:
    agreement_id = Int32().lower_bound(1).as_entity()
    year = Int32().lower_bound(2024).upper_bound(2050).as_entity()
    week = Int32().upper_bound(53).lower_bound(1).as_entity()

    company_id = String()

    variation_ids = List(String()).description("The variation ids")


@feature_view(
    name="preselector_successful_live_output",
    source=databricks_catalog.schema("mloutputs").table("preselector_successful_realtime_output"),
)
class SuccessfulPreselectorOutput:
    billing_agreement_id = Int32().lower_bound(1).as_entity()
    menu_year = Int32().lower_bound(2024).upper_bound(2050).as_entity()
    menu_week = Int32().upper_bound(53).lower_bound(1).as_entity()

    portion_size = Int32().is_optional()
    number_of_recipes = Int32().is_optional()
    company_id = (
        String()
        .accepted_values(
            [
                "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
                "8A613C15-35E4-471F-91CC-972F933331D7",
                "6A2D0B60-84D6-4830-9945-58D518D27AC2",
                "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
            ]
        )
        .is_optional()
    )

    target_cost_of_food_per_recipe = Float()
    error_vector = Struct(PreselectorErrorStructure).is_optional()

    weeks_since_selected = Struct().is_optional().description("Contains the main_recipe_id and the year week")

    recipes = List(Struct(PreselectorRecipeOutput)).is_optional().description("The outputted data")

    main_recipe_ids = List(Int32()).description("This will be deprecated for `recipes`")
    variation_ids = List(String()).description("This will be deprecated for `recipes`")
    compliancy = Int32().lower_bound(0).upper_bound(4).description("This will be deprecated for `recipes`")

    concept_preference_ids = List(String()).description("The concept preference ids used to generate the data")

    generated_at = EventTimestamp()

    taste_preferences = List(String()).is_optional().description("The negative prefs")
    taste_preference_ids = List(String()).is_optional().description("The negative prefs")

    model_version = String()
    has_data_processing_consent = Bool()
    override_deviation = Bool()


@feature_view(
    name="preselector_failed_realtime_output",
    source=databricks_catalog.schema("mloutputs").table("preselector_failed_realtime_output"),
)
class FailedPreselectorOutput:
    error_message = String()
    error_code = Int32()
    request = String()
    "Contains the original request as a Json object"


def preselector_contracts() -> FeatureStore:
    store = FeatureStore.experimental()

    views = [
        RecipePreferences,
        Preselector,
    ]

    for view in views:
        store.add_compiled_view(view.compile())

    return store
