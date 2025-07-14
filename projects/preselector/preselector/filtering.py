import logging
from typing import Annotated

import polars as pl
from aligned import (
    ContractStore,
)
from data_contracts.recipe import (
    NormalizedRecipeFeatures,
    RecipeNegativePreferences,
)

from preselector.data.models.customer import (
    PreselectorPreferenceCompliancy,
)
from preselector.monitor import duration
from preselector.schemas.batch_request import GenerateMealkitRequest

logger = logging.getLogger(__name__)


def filter_out_unwanted_preselected_recipes(recipes: Annotated[pl.DataFrame, NormalizedRecipeFeatures]) -> pl.DataFrame:
    """
    Filters out recipes that we do not want to be in the preselected basket.
    This could either be cheep recipes - to not rip the customer off, or premium recipes
    """

    schema = NormalizedRecipeFeatures()
    not_include_features = [
        schema.is_adams_signature,
        schema.is_cheep,
        schema.is_red_cross,
        schema.is_slow_grown_chicken,
        schema.is_value_add,
    ]
    not_include = [feat.name for feat in not_include_features]
    return recipes.filter(
        pl.reduce(lambda left, right: left & right, [pl.col(col).not_() for col in not_include])
    ).select(pl.exclude(not_include))


async def filter_out_recipes_based_on_preference(
    recipes: pl.DataFrame, portion_size: int, taste_preference_ids: list[str], store: ContractStore
) -> pl.DataFrame:
    """
    Filters out any recipes that conflict with a hard filter rule.
    Or also known as a taste preferences.

    Returns:
        pl.DataFrame: The recipes that do not conflict with the taste preferences
    """
    with duration("load-recipe-information"):
        preferences = (
            await store.feature_view(RecipeNegativePreferences)
            .select({"recipe_id", "preference_ids"})
            .features_for(recipes.with_columns(pl.lit(portion_size).alias("portion_size")))
            .to_polars()
        ).with_columns(
            pl.col("preference_ids").fill_null(
                # Adding the non-vegetarian preference if there are no preferences
                ["870C7CEA-9D06-4F3E-9C9B-C2C395F5E4F5"]
            )
        )

        upper_and_lower = {preference.lower() for preference in taste_preference_ids}.union(
            {preference.upper() for preference in taste_preference_ids},
        )

        logger.debug(f"Filtering based on taste preferences: {recipes.height}")
        acceptable_recipe_ids = (
            preferences.lazy()
            .select(["recipe_id", "preference_ids"])
            .explode(columns=["preference_ids"])
            .with_columns(contains_pref=pl.col("preference_ids").is_in(upper_and_lower))
            .group_by(["recipe_id"])
            .agg(pl.sum("contains_pref").alias("taste_conflicts"))
            .filter(pl.col("taste_conflicts") == 0)
            .unique("recipe_id")
            .collect()
        )
        return recipes.filter(
            pl.col("recipe_id").is_in(acceptable_recipe_ids["recipe_id"]),
        )


async def filter_on_preferences(
    customer: GenerateMealkitRequest, recipes: pl.DataFrame, store: ContractStore
) -> tuple[pl.DataFrame, PreselectorPreferenceCompliancy, pl.DataFrame | None]:
    """
    Filters out all recipes that a customer do not want based on their negative preferences

    This also makes sure that if there is too few recipes will it loosen the preferences and change the compliancy.

    Args:
        customer (GenerateMealkitRequest): The customer requests which contains the negative prefs
        recipes (pl.DataFrame): All the recipes that we could potentially select for a week
        store (ContractStore): A store with all other sources

    Returns:
        A tuple containing the available recipes to choose from, a compliancy value,
        and preselected recipes if we loosen up the preferences.
    """
    if not customer.taste_preferences:
        return recipes, PreselectorPreferenceCompliancy.all_compliant, None

    recipes_to_use = await filter_out_recipes_based_on_preference(
        recipes, portion_size=customer.portion_size, taste_preference_ids=customer.taste_preference_ids, store=store
    )

    if recipes_to_use.height >= customer.number_of_recipes:
        return recipes_to_use, PreselectorPreferenceCompliancy.all_compliant, None

    logger.debug(f"Found only {recipes_to_use.height} recipes that complied with all negative preferences")

    preselected_recipe_df = recipes_to_use.select("main_recipe_id").with_columns(
        compliancy=pl.lit(PreselectorPreferenceCompliancy.all_compliant)
    )

    recipes_to_use = await filter_out_recipes_based_on_preference(
        recipes,
        portion_size=customer.portion_size,
        taste_preference_ids=[pref.preference_id for pref in customer.taste_preferences if pref.is_allergy],
        store=store,
    )

    if recipes_to_use.height >= customer.number_of_recipes:
        return recipes_to_use, PreselectorPreferenceCompliancy.allergies_only_compliant, preselected_recipe_df

    logger.debug(f"Found only {recipes_to_use.height} recipes that complied with allergens")

    preselected_recipe_df = preselected_recipe_df.vstack(
        recipes_to_use.filter(pl.col("main_recipe_id").is_in(preselected_recipe_df["main_recipe_id"]).not_())
        .select("main_recipe_id")
        .with_columns(compliancy=pl.lit(PreselectorPreferenceCompliancy.allergies_only_compliant))
    )
    return recipes, PreselectorPreferenceCompliancy.non_preference_compliant, preselected_recipe_df
