from datetime import date, timedelta
from typing import TypeVar

import polars as pl
from aligned import (
    Bool,
    ContractStore,
    CustomMethodDataSource,
    EventTimestamp,
    Float,
    Int32,
    String,
    feature_view,
)
from aligned.data_source.batch_data_source import DummyDataSource
from aligned.feature_store import FeatureViewStore
from aligned.feature_view.feature_view import FeatureViewWrapper
from aligned.schemas.feature_view import RetrivalRequest
from data_contracts.orders import HistoricalRecipeOrders
from data_contracts.recipe import NormalizedRecipeFeatures, RecipeMainIngredientCategory
from data_contracts.sources import materialized_data

T = TypeVar("T")


def with_freshness(view: FeatureViewWrapper[T], acceptable_freshness: timedelta | None) -> FeatureViewWrapper[T]:
    view.metadata.acceptable_freshness = acceptable_freshness
    return view


recipe_features = NormalizedRecipeFeatures()
recipe_nutrition = NormalizedRecipeFeatures()
recipe_cost = NormalizedRecipeFeatures()
recipe_main_ingredient = RecipeMainIngredientCategory()

fat_agg = recipe_nutrition.fat_pct.aggregate()
protein_agg = recipe_nutrition.protein_pct.aggregate()
veg_fruit_agg = recipe_nutrition.fruit_veg_fresh_p.aggregate()
fat_saturated_agg = recipe_nutrition.fat_saturated_pct.aggregate()
energy_kcal_agg = recipe_nutrition.energy_kcal_per_portion.aggregate()

number_of_ratings_agg = recipe_features.number_of_ratings_log.aggregate()
ratings_agg = recipe_features.average_rating.aggregate()

recipe_cost_whole_units_agg = recipe_cost.cost_of_food.aggregate()


def mean_of_bool(feature: Bool) -> Float:
    return feature.polars_aggregation(
        pl.col(feature.name).fill_null(False).mean(),
        as_type=Float()
    )


class VariationTags:
    protein = "protein_variation"
    carbohydrate = "carbo_variation"
    quality = "quality"
    time = "time"
    equal_dishes = "equal_dishes"

@feature_view(name="basket_features", source=DummyDataSource())
class BasketFeatures:
    basket_id = Int32().as_entity()

    mean_fat = fat_agg.mean()
    mean_protein = protein_agg.mean()
    mean_veg_fruit = veg_fruit_agg.mean()
    mean_fat_saturated = fat_saturated_agg.mean()

    mean_cost_of_food = recipe_cost_whole_units_agg.mean().is_optional()

    mean_energy = energy_kcal_agg.mean()
    mean_number_of_ratings = number_of_ratings_agg.mean().with_tag(VariationTags.quality)

    mean_ratings = recipe_features.average_rating.polars_aggregation(
        pl.col("average_rating").fill_nan(0).mean(), as_type=Float()
    ).with_tag(VariationTags.quality)

    # std_fat = fat_agg.std()
    # std_protein = protein_agg.std()
    # std_veg_fruit = veg_fruit_agg.std()
    # std_fat_saturated = fat_saturated_agg.std()
    # std_price_category_level = price_category_level_agg.std()
    # std_recipe_cost_whole_units = recipe_cost_whole_units_agg.std()
    # std_energy = energy_kcal_agg.std()
    # std_number_of_ratings = number_of_ratings_agg.std()
    # std_ratings = recipe_features.average_rating.polars_aggregation(
    #     pl.col("average_rating").fill_nan(0).std(), as_type=Float()
    # )

    cooking_time_mean = recipe_features.cooking_time_from.aggregate().mean().with_tag(VariationTags.time)
    # cooking_time_std = recipe_features.cooking_time_from.aggregate().std()

    is_low_calorie = mean_of_bool(recipe_features.is_low_calorie)
    is_chef_choice_percentage = mean_of_bool(recipe_features.is_chefs_choice)
    is_family_friendly_percentage = mean_of_bool(recipe_features.is_family_friendly)
    is_lactose_percentage = mean_of_bool(recipe_features.is_lactose)
    is_gluten_free_percentage = mean_of_bool(recipe_features.is_gluten_free)
    is_spicy_percentage = mean_of_bool(recipe_features.is_spicy)

    is_ww_percentage = mean_of_bool(recipe_features.is_weight_watchers).description("Makes only sense in Linas")
    is_roede_percentage = mean_of_bool(recipe_features.is_roede).description("Makes only sense in GL")

    # Main Proteins
    # Setting default value to migrate the changes more easily

    is_vegan_percentage = mean_of_bool(recipe_features.is_vegan).with_tag(VariationTags.protein)
    is_vegetarian_percentage = mean_of_bool(recipe_features.is_vegetarian).with_tag(VariationTags.protein)
    is_fish_percentage = mean_of_bool(recipe_features.is_fish).with_tag(VariationTags.protein)

    for protein in recipe_main_ingredient.all_proteins:
        locals()[f"{protein.name}_percentage"] = (mean_of_bool(protein)
            .default_value(0)
            .with_tag(VariationTags.protein)
        )
    # Need to clean-up the hack as the local loop value is added to the class
    del locals()["protein"]

    is_other_protein_percentage = (mean_of_bool(recipe_main_ingredient.is_other_protein)
        .default_value(0)
        .with_tag(VariationTags.protein)
    )

    # Main Carbos
    for carbo in recipe_main_ingredient.all_carbos:
        locals()[f"{carbo.name}_percentage"] = (mean_of_bool(carbo)
            .default_value(0)
            .with_tag(VariationTags.carbohydrate)
        )
    # Need to clean-up the hack as the local loop value is added to the class
    del locals()["carbo"]

    is_other_carbo_percentage = (mean_of_bool(recipe_main_ingredient.is_other_carbo)
        .default_value(0)
        .with_tag(VariationTags.carbohydrate)
    )

    repeated_taxonomies = Float().polars_aggregation_using_features(
        aggregation=(
            pl.col("taxonomy_ids").list.explode().unique_counts().max() / pl.count("recipe_id")
        ),
        using_features=[
            recipe_features.taxonomy_ids,
            recipe_features.recipe_id
        ]
    ).with_tag(VariationTags.equal_dishes)


async def historical_preselector_vector(
    request: RetrivalRequest,
    limit: int | None,
    from_date: date | None = None,
    store: ContractStore | None = None
) -> pl.LazyFrame:
    from datetime import datetime, timedelta, timezone

    number_of_historical_orders = 20
    year_weeks = []
    from_date = from_date or date.today()

    def query(view_wrapper: FeatureViewWrapper) -> FeatureViewStore:
        """
        Makes it easier to swap between prod, and manually defined data for testing.
        """
        if store:
            return store.feature_view(view_wrapper.metadata.name)
        else:
            return view_wrapper.query()


    for i in range(1, number_of_historical_orders):
        year_week = from_date - timedelta(weeks=i)
        year_weeks.append((year_week.year, year_week.isocalendar()[1]))


    history = query(HistoricalRecipeOrders)
    normalized_recipes = query(NormalizedRecipeFeatures)
    main_ingredient_category = query(RecipeMainIngredientCategory)

    year_week_number = [year * 100 + week for year, week in year_weeks]

    df = await history.select_columns([
        "agreement_id",
        "recipe_id",
        "portion_size",
        "year",
        "week",
        "company_id"
    ]).filter(
        (pl.col("year") * 100 + pl.col("week")).is_in(year_week_number),
    ).to_polars()

    norm_features = (
        await normalized_recipes
        .features_for(df)
        .with_subfeatures()
        .to_lazy_polars()
    )

    features = (
        await main_ingredient_category
        .features_for(norm_features)
        .with_subfeatures()
        .to_lazy_polars()
    )

    basket_features = (
        await BasketFeatures.process_input(
            features.with_columns(
                year_week=pl.col("agreement_id") * 1_000_000 + pl.col("year") * 100 + pl.col("week")
            ).rename(
                {
                    "year_week": "basket_id",
                }
            )
        ).to_polars()
    ).with_columns(
        agreement_id=(pl.col("basket_id") / 1_000_000).floor().cast(pl.UInt64),
    )

    assert not basket_features.is_empty(), "Found no basket features"

    exclude_columns = ["basket_id", "agreement_id"]
    feature_columns = [feat for feat in basket_features.columns if feat not in exclude_columns]

    target = (
        basket_features.group_by("agreement_id")
        .agg([pl.col(feat).fill_nan(0).mean().alias(feat) for feat in feature_columns])
        .with_columns(vector_type=pl.lit("target"))
    )

    manually_set_columns = ["mean_cost_of_food"]

    importance = (
        basket_features.filter((pl.count("basket_id") > 1).over("agreement_id"))
        .group_by("agreement_id")
        .agg(
            [
                (
                    1 / pl.col(feat).fill_nan(0).top_k(
                        # Adding top k as noise reduction
                        (pl.len() * 0.75).ceil()
                    ).bottom_k(
                        # using 51 perc so we alwasy have at least 2 rows
                        (pl.len() * 0.51).ceil()
                    ).std()
                    .clip(lower_bound=0.1)
                ).exp().sub(
                    pl.lit(0.5).exp()
                ).alias(feat)
                for feat in feature_columns
            ]
        )
        .with_columns([
            # No need to let cost of food effect the importance, so we set this to 0
            pl.lit(0).alias(feat) for feat in manually_set_columns
        ])
        .with_columns([pl.col(feat) / pl.sum_horizontal(feature_columns) for feat in feature_columns])
        .with_columns(vector_type=pl.lit("importance"))
    )
    return target.vstack(importance).with_columns(created_at=datetime.now(tz=timezone.utc)).lazy()


PredefinedVectors = BasketFeatures.with_schema(
    name="predefined_vectors",
    source=materialized_data.parquet_at("predefined_vectors.parquet"),
    entities=dict(
        concept_id=String(), company_id=String(), vector_type=String().accepted_values(["importance", "target"])
    ),
    additional_features=dict(concept_name=String()),
    copy_default_values=True
)

PreselectorVector = with_freshness(
    BasketFeatures.with_schema(
        name="preselector_vector",
        source=CustomMethodDataSource.from_methods(
            all_data=historical_preselector_vector,
            depends_on_sources={
                NormalizedRecipeFeatures.location,
                HistoricalRecipeOrders.location,
                RecipeMainIngredientCategory.location
            },
        ),
        materialized_source=materialized_data.parquet_at("preselector_vector.parquet"),
        entities=dict(agreement_id=Int32()),
        additional_features=dict(
            vector_type=String().accepted_values(["importance", "target"]), created_at=EventTimestamp()
        ),
    ),
    acceptable_freshness=timedelta(days=6),
)

TargetVectors = PreselectorVector.filter(
    name="target_vector",
    where=pl.col("vector_type") == "target",
    materialize_source=materialized_data.parquet_at("target_vectors.parquet"),
)
ImportanceVector = PreselectorVector.filter(
    name="importance_vector",
    where=pl.col("vector_type") == "importance",
    materialize_source=materialized_data.parquet_at("importance_vector.parquet"),
)
