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
from aligned.feature_store import FeatureViewStore
from aligned.feature_view.feature_view import FeatureViewWrapper
from aligned.schemas.feature_view import RetrivalRequest
from aligned.sources.random_source import RandomDataSource
from data_contracts.orders import HistoricalRecipeOrders, WeeksSinceRecipe
from data_contracts.recipe import NormalizedRecipeFeatures, RecipeMainIngredientCategory
from data_contracts.recommendations.recommendations import RecommendatedDish
from data_contracts.sources import materialized_data

T = TypeVar("T")


def with_freshness(view: FeatureViewWrapper[T], acceptable_freshness: timedelta | None) -> FeatureViewWrapper[T]:
    view.metadata.acceptable_freshness = acceptable_freshness
    return view


recipe_features = NormalizedRecipeFeatures()
recipe_nutrition = NormalizedRecipeFeatures()
recipe_cost = NormalizedRecipeFeatures()
recipe_main_ingredient = RecipeMainIngredientCategory()
recommandations = RecommendatedDish()

fat_agg = recipe_nutrition.fat_pct.aggregate()
protein_agg = recipe_nutrition.protein_pct.aggregate()
veg_fruit_agg = recipe_nutrition.fruit_veg_fresh_p.aggregate()
fat_saturated_agg = recipe_nutrition.fat_saturated_pct.aggregate()
energy_kcal_agg = recipe_nutrition.energy_kcal_per_portion.aggregate()

number_of_ratings_agg = recipe_features.number_of_ratings_log.aggregate()
ratings_agg = recipe_features.average_rating.aggregate()

order_rank_agg = recommandations.order_rank.aggregate()

recipe_cost_whole_units_agg = recipe_cost.cost_of_food.aggregate()


class VariationTags:
    protein = "protein_variation"
    carbohydrate = "carbo_variation"
    quality = "quality"
    time = "time"
    equal_dishes = "equal_dishes"

class PreselectorTags:
    binary_metric = "binary_metric"

def mean_of_bool(feature: Bool) -> Float:
    return feature.polars_aggregation(
        pl.col(feature.name).fill_null(False).mean(),
        as_type=Float()
    ).with_tag(PreselectorTags.binary_metric)


quarantining = WeeksSinceRecipe()

ordered_ago_agg = quarantining.ordered_weeks_ago.aggregate()

@feature_view(
    name="injected_preselector_features",
    source=RandomDataSource()
)
class InjectedFeatures:
    mean_cost_of_food = recipe_cost_whole_units_agg.mean().is_optional()
    mean_rank = order_rank_agg.mean().default_value(0)
    mean_ordered_ago = ordered_ago_agg.mean().default_value(1)


injected_features = InjectedFeatures()

@feature_view(name="basket_features", source=RandomDataSource())
class BasketFeatures:
    basket_id = Int32().as_entity()

    mean_fat = fat_agg.mean()
    mean_protein = protein_agg.mean()
    mean_veg_fruit = veg_fruit_agg.mean()
    mean_fat_saturated = fat_saturated_agg.mean()

    mean_rank = injected_features.mean_rank
    mean_cost_of_food = injected_features.mean_cost_of_food
    mean_ordered_ago = injected_features.mean_ordered_ago.default_value(0)

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

    is_seefood_percentage = mean_of_bool(
        recipe_main_ingredient.is_seefood
    ).with_tag(VariationTags.protein).default_value(0)

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
            pl.col("taxonomy_of_interest").list.explode().unique_counts().quantile(0.9) / pl.count("recipe_id")
        ),
        using_features=[
            recipe_features.taxonomy_of_interest,
            recipe_features.recipe_id
        ]
    ).with_tag(VariationTags.equal_dishes)



async def historical_customer_mealkit_features(
    request: RetrivalRequest,
    from_date: date | None = None,
    store: ContractStore | None = None
) -> pl.LazyFrame:

    from datetime import timedelta

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

    inject_features = InjectedFeatures.query().request.aggregated_features
    needed_dummy_features = []
    for feature in inject_features:
        needed_dummy_features.extend(feature.depending_on_names)

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

    features = features.with_columns([
        pl.lit(1).alias(feat)
        for feat in needed_dummy_features
        if feat not in features.columns
    ])

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
        year=((pl.col("basket_id") % 1_000_000) / 100).floor().cast(pl.UInt64),
        week=(pl.col("basket_id") % 100)
    )

    assert not basket_features.is_empty(), "Found no basket features"
    return basket_features.lazy()



HistoricalCustomerMealkitFeatures = BasketFeatures.with_schema(
    name="historical_customer_mealkit_features",
    source=CustomMethodDataSource.from_load(
        method=historical_customer_mealkit_features,
        depends_on={
            NormalizedRecipeFeatures.location,
            HistoricalRecipeOrders.location,
            RecipeMainIngredientCategory.location
        }
    ),
    entities=dict(
        agreement_id=Int32(),
        year=Int32(),
        week=Int32()
    ),
    materialized_source=materialized_data.parquet_at("historical_customer_mealkit_features.parquet")
)

async def historical_preselector_vector(
    request: RetrivalRequest,
    limit: int | None,
    store: ContractStore | None = None
) -> pl.LazyFrame:
    from datetime import datetime, timezone

    def query(view_wrapper: FeatureViewWrapper) -> FeatureViewStore:
        """
        Makes it easier to swap between prod, and manually defined data for testing.
        """
        if store:
            return store.feature_view(view_wrapper.metadata.name)
        else:
            return view_wrapper.query()

    basket_features = await query(HistoricalCustomerMealkitFeatures).all().to_polars()

    inject_features = InjectedFeatures.query().request.all_feature_names
    exclude_columns = ["basket_id", "agreement_id"]

    features = {
        feat
        for feat in BasketFeatures.query().request.all_features
        if feat.name not in inject_features
    }
    feature_columns = [
        feat.name
        for feat
        in features
        if feat not in exclude_columns
    ]
    scalar_feature_columns = [
        feat.name
        for feat
        in features
        if feat not in exclude_columns and (feat.tags is None or PreselectorTags.binary_metric not in feat.tags)
    ]
    boolean_feature_columns = [
        feat.name
        for feat
        in features
        if feat not in exclude_columns and feat.tags is not None and PreselectorTags.binary_metric in feat.tags
    ]
    target = (
        basket_features.group_by("agreement_id")
        .agg([pl.col(feat).fill_nan(0).mean().alias(feat) for feat in feature_columns])
        .with_columns(vector_type=pl.lit("target"))
    )

    importance = (
        basket_features.filter((pl.len() > 1).over("agreement_id"))
        .group_by("agreement_id")
        .agg([
            *[
                (
                    1 / pl.col(feat).fill_nan(0).top_k(
                        # Adding top k as noise reduction
                        (pl.len() * 0.75).ceil()
                    ).bottom_k(
                        # using 51 perc so we alwasy have at least 2 rows
                        (pl.len() * 0.51).ceil()
                    ).std()
                    .clip(lower_bound=0.1)
                ).alias(feat)
                for feat in scalar_feature_columns
            ],
            *[
                (
                    (pl.col(feat).filter(
                        pl.col(feat) > 0
                    ).len() / pl.col(feat).len() - 0.4) / 0.4
                ).abs().clip(lower_bound=0, upper_bound=1).mul(10).alias(feat)
                for feat in boolean_feature_columns
            ]
        ])
        .with_columns([pl.col(feat) / pl.sum_horizontal(scalar_feature_columns) for feat in feature_columns ])
        .with_columns(vector_type=pl.lit("importance"))
    )
    return target.vstack(
        importance.select(target.columns)
    ).with_columns(
        created_at=datetime.now(tz=timezone.utc),
        **{
            feat: 0
            for feat in inject_features
        }
    ).lazy()


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
            depends_on_sources={HistoricalCustomerMealkitFeatures.location},
        ),
        materialized_source=materialized_data.parquet_at("preselector_vector.parquet"),
        entities=dict(agreement_id=Int32()),
        additional_features=dict(
            vector_type=String().accepted_values(["importance", "target"]), created_at=EventTimestamp()
        ),
        copy_default_values=True
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
