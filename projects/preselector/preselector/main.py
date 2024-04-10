import logging
from typing import Annotated

import pandas as pd
import polars as pl
from aligned import FileSource, Int32, String, check_schema, feature_view
from aligned.compiler.aggregation_factory import PolarsTransformationFactoryAggregation
from aligned.compiler.feature_factory import FeatureFactory
from data_contracts.recommendations.recipe import (
    RecipeCost,
    RecipeFeatures,
    RecipeNutrition,
)
from data_contracts.recommendations.recommendations import RecommendatedDish

from preselector.data.models.customer import PreselectorCustomer, PreselectorResult

logger = logging.getLogger(__name__)


recipe_features = RecipeFeatures()
recipe_nutrition = RecipeNutrition()
recipe_cost = RecipeCost()


def custom_aggregation(
    method: pl.Expr,
    using_features: list[FeatureFactory],
    as_dtype: FeatureFactory,
) -> FeatureFactory:
    as_dtype.transformation = PolarsTransformationFactoryAggregation(
        as_dtype,
        method=method,
        _using_features=using_features,
    )
    return as_dtype


@feature_view(
    name="basket_features",
    source=FileSource.parquet_at("data.parquet"),
)
class BasketFeatures:
    basket_id = Int32().as_entity()

    max_cooking_time_agg = recipe_features.cooking_time_from.aggregate()

    energy_kcal_agg = recipe_nutrition.energy_kcal_100g.aggregate()
    fat_agg = recipe_nutrition.fat_100g.aggregate()
    fat_saturated_agg = recipe_nutrition.fat_saturated_100g.aggregate()
    protein_agg = recipe_nutrition.protein_100g.aggregate()
    veg_fruit_agg = recipe_nutrition.fruit_veg_fresh_100g.aggregate()
    price_category_level_agg = recipe_cost.price_category_level.aggregate()
    recipe_cost_whole_units_agg = recipe_cost.recipe_cost_whole_units.aggregate()

    mean_energy = energy_kcal_agg.mean()
    mean_fat = fat_agg.mean()
    mean_fat_saturated = fat_saturated_agg.mean()
    mean_coocking_time = max_cooking_time_agg.mean()
    mean_protein = protein_agg.mean()
    mean_veg_fruit = veg_fruit_agg.mean()
    mean_price_category_level = price_category_level_agg.mean()
    mean_recipe_cost_whole_units = recipe_cost_whole_units_agg.mean()

    median_cooking_time = max_cooking_time_agg.median()

    std_cooking_time = max_cooking_time_agg.std()
    std_energy = energy_kcal_agg.std()
    std_fat = fat_agg.std()
    std_fat_saturated = fat_saturated_agg.std()
    std_protein = protein_agg.std()
    std_veg_fruit = veg_fruit_agg.std()
    std_price_category_level = price_category_level_agg.std()
    std_recipe_cost_whole_units = recipe_cost_whole_units_agg.std()

    is_family_friendly_mean = custom_aggregation(
        (pl.col("is_family_friendly") | pl.col("is_kids_friendly")).mean(),
        using_features=[
            recipe_features.is_family_friendly,
            recipe_features.is_kids_friendly,
        ],
        as_dtype=Int32(),
    )


def select_next_vector(
    current_vector: pl.Series,
    target_vector: pl.Series,
    available_vectors: pl.DataFrame,
    normalization_vector: pl.Series,
    columns: list[str],
    exclude_column: str = "basket_id",
    rename_column: str = "recipe_id",
) -> pl.DataFrame:
    optimal_vector = target_vector - current_vector

    distance = (
        available_vectors.select(pl.exclude(exclude_column))
        .select(columns)
        .transpose()
        .lazy()
        .select(((pl.all() - optimal_vector) / normalization_vector).pow(2).sum())
        .collect()
        .transpose()
    )

    distance = available_vectors.with_columns(distance=distance["column_0"]).sort("distance", descending=False).limit(1)
    return distance.select(
        pl.exclude(["distance", exclude_column]),
        pl.col(exclude_column).alias(rename_column),
    )


async def compute_vector(
    df: pl.DataFrame,
    column_order: list[str],
    basket_column: str | None = None,
) -> pl.Series:
    if not basket_column:
        df = df.with_columns(pl.lit(1).alias("basket_id"))
    else:
        df = df.with_columns(pl.col(basket_column).alias("basket_id"))

    return (
        (await BasketFeatures.process_input(df).to_polars())
        .select(pl.exclude("basket_id"))
        .select(column_order)
        .fill_null(0)
        .transpose()
        .to_series()
    )


async def find_best_combination(
    target_combination_values: pl.DataFrame,
    available_recipes: Annotated[pl.DataFrame, RecipeFeatures],
    number_of_recipes: int,
) -> tuple[list[int], Annotated[dict, "Soft preference error"]]:
    final_combination = pl.DataFrame()

    columns = target_combination_values.columns

    normalization_vector = await compute_vector(available_recipes, columns)

    if 0 in normalization_vector:
        normalization_vector = normalization_vector.replace(0, 1)

    target_combination = target_combination_values.transpose().to_series()
    current_vector = pl.Series(values=[0] * target_combination.shape[0])

    recipes_to_choose_from = available_recipes

    # Sorting in order to get deterministic results
    recipe_nudge = (
        await BasketFeatures.process_input(
            recipes_to_choose_from.with_columns(basket_id=pl.col("recipe_id")),
        ).to_polars()
    ).sort("basket_id", descending=False)

    for _ in range(number_of_recipes):
        next_vector = select_next_vector(
            current_vector,
            target_combination,
            recipe_nudge,
            normalization_vector,
            columns,
        )

        selected_recipe_id = next_vector[0, "recipe_id"]
        final_combination = final_combination.vstack(
            recipes_to_choose_from.filter(pl.col("recipe_id") == selected_recipe_id),
        )

        current_vector = await compute_vector(final_combination, columns)

        recipes_to_choose_from = recipes_to_choose_from.filter(
            pl.col("recipe_id") != selected_recipe_id,
        )

        raw_recipe_nudge = recipes_to_choose_from.with_columns(
            pl.col("recipe_id").alias("basket_id"),
        )

        for recipe_id in raw_recipe_nudge["recipe_id"].to_list():
            raw_recipe_nudge = raw_recipe_nudge.vstack(
                final_combination.with_columns(pl.lit(recipe_id).alias("basket_id")),
            )

        # Sorting in order to get deterministic results
        recipe_nudge = (await BasketFeatures.process_input(raw_recipe_nudge).to_polars()).sort(
            "basket_id",
            descending=False,
        )

    error = dict(
        zip(columns, (current_vector - target_combination).to_list(), strict=False),
    )

    # Error debugging in Streamlit
    # st.write(((current_vector - target_combination) / normalization_vector).pow(2).sum())

    return (final_combination["recipe_id"].to_list(), error)


@feature_view(
    name="menu",
    source=FileSource.parquet_at("data.parquet"),
)
class Menu:
    recipe_id = Int32().as_entity()
    menu_week = Int32()
    menu_year = Int32()

    main_recipe_id = Int32()
    variation_id = String()
    product_id = String()

    variation_portions = Int32()


@check_schema()
async def run_preselector_with_menu(
    customer: PreselectorCustomer,
    menu: Annotated[pd.DataFrame, Menu],
    target_vector: pl.DataFrame,
    recommendations: Annotated[pd.DataFrame, RecommendatedDish],
) -> PreselectorResult:
    selected_recipe_ids = await run_preselector(
        customer,
        pl.from_pandas(menu),
        pl.from_pandas(recommendations),
        target_vector=target_vector,
    )

    return PreselectorResult(
        agreement_id=customer.agreement_id,
        main_recipe_ids=selected_recipe_ids,
        debug_summary={},
    )


async def run_preselector(
    customer: PreselectorCustomer,
    available_recipes: Annotated[pl.DataFrame, Menu],
    recommendations: Annotated[pl.DataFrame, RecommendatedDish],
    target_vector: pl.DataFrame,
    select_top_n: int = 20,
) -> list[int]:
    from data_contracts.preselector.store import RecipePreferences
    from data_contracts.recommendations.store import recommendation_feature_contracts

    from preselector.recipe_contracts import Preselector

    store = recommendation_feature_contracts()
    store.add_compiled_model(Preselector.compile())

    recipes = available_recipes

    logger.info(f"Filtering based on portion size: {recipes.height}")
    recipes = recipes.filter(
        pl.col("variation_portions").cast(pl.Int32) == customer.portion_size,
    )
    logger.info(f"Filtering based on portion size done: {recipes.height}")

    if customer.taste_preference_ids:
        preferences = (
            await RecipePreferences.query()
            .select({"recipe_id", "preference_ids"})
            .features_for(available_recipes)
            .to_polars()
        )

        upper_and_lower = {preference.lower() for preference in customer.taste_preference_ids}.union(
            {preference.upper() for preference in customer.taste_preference_ids},
        )

        logger.info(f"Filtering based on taste preferences: {recipes.height}")
        acceptable_recipe_ids = (
            preferences.lazy()
            .select(["recipe_id", "preference_ids"])
            .explode(columns=["preference_ids"])
            .with_columns(contains_pref=pl.col("preference_ids").is_in(upper_and_lower))
            .groupby(["recipe_id"])
            .agg(pl.sum("contains_pref").alias("taste_conflicts"))
            .filter(pl.col("taste_conflicts") == 0)
            .unique("recipe_id")
            .collect()
        )
        recipes = recipes.filter(
            pl.col("recipe_id").is_in(acceptable_recipe_ids["recipe_id"]),
        )
        logger.info(f"Filtering based on taste preferences done: {recipes.height}")

    logger.info(f"Loading preselector recipe features: {recipes.height}")
    recipe_features = await (
        store.model("preselector")
        .features_for(
            recipes.with_columns(pl.lit(customer.portion_size).alias("portion_size")),
        )
        .to_polars()
    )

    logger.info(f"Filtering out cheep and premium recipes: {recipe_features.height}")
    recipe_features = recipe_features.filter(pl.col("is_premium").is_not() & pl.col("is_cheep").is_not()).select(
        pl.exclude(["is_premium", "is_cheep"]),
    )
    logger.info(f"Filtered out cheep and premium recipes: {recipe_features.height}")

    if recommendations.height > select_top_n:
        logger.info(f"Selecting top {select_top_n} based on recommendations: {recipe_features.height}")
        product_ids = recipes["product_id"].to_list()

        top_n_recipes = (
            recommendations.filter(pl.col("product_id").is_in(product_ids))
            .sort("order_of_relevance_cluster", descending=False)
            .limit(select_top_n)
        )

        recipes = recipes.filter(
            pl.col("product_id").is_in(top_n_recipes["product_id"]),
        )
        recipe_features = recipe_features.filter(
            pl.col("recipe_id").is_in(recipes["recipe_id"]),
        )
        logger.info(f"Filtering based on recommendations done: {recipe_features.height}")

    logger.info(f"Selecting the best combination based on {recipe_features.height} recipes.")
    best_recipe_ids, error = await find_best_combination(
        target_vector,
        recipe_features,
        customer.number_of_recipes,
    )

    logging.info(f"Preselector Error: {error}")

    return recipes.filter(pl.col("recipe_id").is_in(best_recipe_ids))["main_recipe_id"].to_list()
