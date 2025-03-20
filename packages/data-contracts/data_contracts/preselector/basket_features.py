from datetime import date, timedelta
from typing import TypeVar

import polars as pl
from aligned import (
    Bool,
    ContractStore,
    CustomMethodDataSource,
    EventTimestamp,
    Float,
    Float64,
    Int32,
    String,
    feature_view,
)
from aligned.compiler.feature_factory import FeatureFactory
from aligned.feature_store import FeatureViewStore
from aligned.feature_view.feature_view import FeatureViewWrapper
from aligned.schemas.feature_view import RetrievalRequest
from aligned.sources.in_mem_source import InMemorySource
from aligned.sources.random_source import RandomDataSource
from data_contracts.attribute_scoring import AttributeScoring
from data_contracts.orders import HistoricalRecipeOrders, WeeksSinceRecipe
from data_contracts.recipe import MealkitRecipeSimilarity, NormalizedRecipeFeatures, RecipeMainIngredientCategory
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
recommendations = RecommendatedDish()
mealkit_recipe_similiarty = MealkitRecipeSimilarity()
attribute_scoring = AttributeScoring()

fat_agg = recipe_nutrition.fat_pct.aggregate()
protein_agg = recipe_nutrition.protein_pct.aggregate()
veg_fruit_agg = recipe_nutrition.fruit_veg_fresh_p.aggregate()
fat_saturated_agg = recipe_nutrition.fat_saturated_pct.aggregate()
energy_kcal_agg = recipe_nutrition.energy_kcal_per_portion.aggregate()

number_of_ratings_agg = recipe_features.number_of_ratings_log.aggregate()
ratings_agg = recipe_features.average_rating.aggregate()

order_rank_agg = recommendations.order_rank.aggregate()

recipe_cost_whole_units_agg = recipe_cost.cost_of_food.aggregate()

is_family_friendly_agg = attribute_scoring.family_friendly_probability.aggregate()


class VariationTags:
    protein = "protein_variation"
    carbohydrate = "carbo_variation"
    quality = "quality"
    time = "time"
    equal_dishes = "equal_dishes"


class PreselectorTags:
    binary_metric = "binary_metric"


def mean_of_bool(feature: Bool) -> Float64:
    return feature.polars_aggregation(pl.col(feature.name).fill_null(False).mean(), as_type=Float64()).with_tag(
        PreselectorTags.binary_metric
    )


def cast_feature(feature: FeatureFactory, dtype: T) -> T:
    assert isinstance(dtype, FeatureFactory)
    dtype.transformation = feature.transformation
    return dtype


quarantining = WeeksSinceRecipe()

ordered_ago_agg = quarantining.ordered_weeks_ago.aggregate()


@feature_view(name="injected_preselector_features", source=RandomDataSource())
class InjectedFeatures:
    mean_cost_of_food = recipe_cost_whole_units_agg.mean().is_optional()
    mean_rank = order_rank_agg.mean().default_value(0)
    mean_ordered_ago = ordered_ago_agg.mean().default_value(1)
    intra_week_similarity = Float().default_value(0)
    repeated_proteins_percentage = Float().default_value(0)
    repeated_carbo_percentage = Float().default_value(0)


injected_features = InjectedFeatures()


@feature_view(name="basket_features", source=RandomDataSource())
class BasketFeatures:
    basket_id = Int32().as_entity()

    mean_fat = cast_feature(fat_agg.mean(), Float64())
    mean_protein = cast_feature(protein_agg.mean(), Float64())
    mean_veg_fruit = cast_feature(veg_fruit_agg.mean(), Float64())
    mean_fat_saturated = cast_feature(fat_saturated_agg.mean(), Float64())

    mean_rank = cast_feature(injected_features.mean_rank, Float64())
    mean_cost_of_food = cast_feature(injected_features.mean_cost_of_food, Float64())
    mean_ordered_ago = cast_feature(injected_features.mean_ordered_ago.default_value(0), Float64())
    intra_week_similarity = cast_feature(injected_features.intra_week_similarity, Float64())

    mean_energy = cast_feature(energy_kcal_agg.mean(), Float64())
    mean_number_of_ratings = cast_feature(number_of_ratings_agg.mean().with_tag(VariationTags.quality), Float64())

    mean_ratings = recipe_features.average_rating.polars_aggregation(
        pl.col("average_rating").fill_nan(0).mean(), as_type=Float64()
    ).with_tag(VariationTags.quality)

    mean_family_friendly_probability = cast_feature(is_family_friendly_agg.mean().default_value(0), Float64())

    cooking_time_mean = cast_feature(
        recipe_features.cooking_time_from.aggregate().mean().with_tag(VariationTags.time), Float64()
    )

    is_low_calorie = mean_of_bool(recipe_features.is_low_calorie)
    is_chef_choice_percentage = mean_of_bool(recipe_features.is_chefs_choice)
    is_family_friendly_percentage = mean_of_bool(recipe_features.is_family_friendly)
    is_lactose_percentage = mean_of_bool(recipe_features.is_lactose)
    is_gluten_free_percentage = mean_of_bool(recipe_features.is_gluten_free)
    is_spicy_percentage = mean_of_bool(recipe_features.is_spicy)

    is_roede_percentage = mean_of_bool(recipe_features.is_roede).description("Makes only sense in GL")

    # Main Proteins
    # Setting default value to migrate the changes more easily
    repeated_proteins_percentage = recipe_features.main_ingredient_id.polars_aggregation(
        # Will always be at least one repeated protein
        pl.col("main_ingredient_id").unique_counts().mean() / pl.count("main_ingredient_id"),
        as_type=Float64(),
    ).default_value(0)

    is_vegan_percentage = mean_of_bool(recipe_features.is_vegan).with_tag(VariationTags.protein)
    is_vegetarian_percentage = mean_of_bool(recipe_features.is_vegetarian).with_tag(VariationTags.protein)

    is_seafood_percentage = (
        Float64()
        .polars_aggregation_using_features(
            [recipe_main_ingredient.is_seafood, recipe_features.is_fish],
            (pl.col("is_seafood") | pl.col("is_fish")).fill_null(False).mean(),
        )
        .with_tag(VariationTags.protein)
        .with_tag(PreselectorTags.binary_metric)
        .default_value(0)
    )

    for protein in recipe_main_ingredient.all_proteins:
        locals()[f"{protein.name}_percentage"] = mean_of_bool(protein).default_value(0).with_tag(VariationTags.protein)
    # Need to clean-up the hack as the local loop value is added to the class
    del locals()["protein"]

    is_other_protein_percentage = (
        mean_of_bool(recipe_main_ingredient.is_other_protein).default_value(0).with_tag(VariationTags.protein)
    )

    # Main Carbos
    for carbo in recipe_main_ingredient.all_carbos:
        locals()[f"{carbo.name}_percentage"] = mean_of_bool(carbo).default_value(0).with_tag(VariationTags.carbohydrate)
    # Need to clean-up the hack as the local loop value is added to the class
    del locals()["carbo"]

    is_other_carbo_percentage = (
        mean_of_bool(recipe_main_ingredient.is_other_carbo).default_value(0).with_tag(VariationTags.carbohydrate)
    )

    # Main Carbo
    # Setting default value to migrate the changes more easily
    repeated_carbo_percentage = recipe_main_ingredient.main_carbohydrate_category_id.polars_aggregation(
        # Will always be at least one repeated protein
        pl.col("main_carbohydrate_category_id").unique_counts().mean() / pl.count("main_carbohydrate_category_id"),
        as_type=Float64(),
    ).default_value(0)


async def historical_customer_mealkit_features(
    request: RetrievalRequest, from_date: date | None = None, store: ContractStore | None = None
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
        year_weeks.append((year_week.year, year_week.isocalendar().week))

    history = query(HistoricalRecipeOrders)
    normalized_recipes = query(NormalizedRecipeFeatures)
    main_ingredient_category = query(RecipeMainIngredientCategory)
    attribute_scoring = query(AttributeScoring)
    inject_request = InjectedFeatures.query().request
    inject_features = inject_request.aggregated_features
    needed_dummy_features = []
    for feature in inject_features:
        needed_dummy_features.extend(feature.depending_on_names)

    year_week_number = [year * 100 + week for year, week in year_weeks]

    df = (
        await history.select_columns(["agreement_id", "recipe_id", "portion_size", "year", "week", "company_id"])
        .filter(
            (pl.col("year") * 100 + pl.col("week")).is_in(year_week_number),
        )
        .to_polars()
    )

    norm_features = await normalized_recipes.features_for(df).with_subfeatures().to_lazy_polars()

    features = await main_ingredient_category.features_for(norm_features).with_subfeatures().to_lazy_polars()
    features = await attribute_scoring.features_for(features).with_subfeatures().to_lazy_polars()

    features = features.with_columns(
        [pl.lit(1).alias(feat) for feat in needed_dummy_features if feat not in features.columns]
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
        year=((pl.col("basket_id") % 1_000_000) / 100).floor().cast(pl.UInt64),
        week=(pl.col("basket_id") % 100),
    )

    for feat in inject_request.features_to_include:
        basket_features = basket_features.with_columns(pl.lit(0).alias(feat))

    assert not basket_features.is_empty(), "Found no basket features"
    return basket_features.lazy()


HistoricalCustomerMealkitFeatures = BasketFeatures.with_schema(
    name="historical_customer_mealkit_features",
    source=CustomMethodDataSource.from_load(
        method=historical_customer_mealkit_features,
        depends_on={
            NormalizedRecipeFeatures.location,
            HistoricalRecipeOrders.location,
            RecipeMainIngredientCategory.location,
        },
    ),
    entities=dict(agreement_id=Int32(), year=Int32(), week=Int32()),
    materialized_source=materialized_data.parquet_at("historical_customer_mealkit_features.parquet"),
)


async def historical_preselector_vector(
    request: RetrievalRequest, limit: int | None, store: ContractStore | None = None
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

    features = {feat for feat in BasketFeatures.query().request.all_features if feat.name not in inject_features}
    feature_columns = [feat.name for feat in features if feat not in exclude_columns]
    scalar_feature_columns = [
        feat.name
        for feat in features
        if feat not in exclude_columns and (feat.tags is None or PreselectorTags.binary_metric not in feat.tags)
    ]
    boolean_feature_columns = [
        feat.name
        for feat in features
        if feat not in exclude_columns and feat.tags is not None and PreselectorTags.binary_metric in feat.tags
    ]
    target = (
        basket_features.group_by("agreement_id")
        .agg([pl.col(feat).fill_nan(0).mean().alias(feat).cast(pl.Float32) for feat in feature_columns])
        .with_columns(vector_type=pl.lit("target"))
    )

    center_point = 0.3
    buffer_range = 0.2

    importance = (
        basket_features.filter((pl.len() > 1).over("agreement_id"))
        .group_by("agreement_id")
        .agg(
            [
                *[
                    (1 / pl.col(feat).fill_nan(0).std().clip(lower_bound=0.1)).alias(feat)
                    for feat in scalar_feature_columns
                ],
                *[
                    ((pl.col(feat).filter(pl.col(feat) > 0).len() / pl.col(feat).len() - center_point) / buffer_range)
                    .abs()
                    .clip(lower_bound=0, upper_bound=1)
                    .mul(10)
                    .alias(feat)
                    for feat in boolean_feature_columns
                ],
            ]
        )
        .with_columns(
            [(pl.col(feat) / pl.sum_horizontal(feature_columns)).cast(pl.Float32) for feat in feature_columns]
        )
        .with_columns(vector_type=pl.lit("importance"))
    )
    return (
        target.vstack(importance.select(target.columns))
        .with_columns(created_at=datetime.now(tz=timezone.utc), **{feat: 0 for feat in inject_features})
        .lazy()
    )


async def generate_attribut_definition_vectors(request: RetrievalRequest) -> pl.LazyFrame:
    from dataclasses import dataclass

    @dataclass
    class FeatureImportance:
        target: float
        importance: float

    companies = {
        "AMK": "8A613C15-35E4-471F-91CC-972F933331D7",
        "GL": "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
        "LMK": "6A2D0B60-84D6-4830-9945-58D518D27AC2",
        "RT": "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
    }

    default_values = {
        # Quick and easy
        "C28F210B-427E-45FA-9150-D6344CAE669B": {
            "cooking_time_mean": FeatureImportance(target=0.0, importance=1.0),
        },
        # Chef favorite
        "C94BCC7E-C023-40CE-81E0-C34DA3D79545": {
            "is_chef_choice_percentage": FeatureImportance(target=1.0, importance=0.1),
            # "mean_number_of_ratings": FeatureImportance(target=0.75, importance=0.1),
            # "mean_ratings": FeatureImportance(target=0.8, importance=0.1),
            "mean_family_friendly_probability": FeatureImportance(target=0.0, importance=1.0),
        },
        # Family
        "B172864F-D58E-4395-B182-26C6A1F1C746": {
            "is_family_friendly_percentage": FeatureImportance(target=1.0, importance=0.1),
            "mean_family_friendly_probability": FeatureImportance(target=1.0, importance=1.0),
        },
        # Vegetarian
        "6A494593-2931-4269-80EE-470D38F04796": {
            "is_vegetarian_percentage": FeatureImportance(target=1.0, importance=1.0),
        },
        # Low Cal
        "FD661CAD-7F45-4D02-A36E-12720D5C16CA": {
            "is_low_calorie": FeatureImportance(target=1.0, importance=1.0),
        },
        # Roede
        "DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA": {
            "is_roede_percentage": FeatureImportance(target=1.0, importance=1.0),
        },
        # Single
        "37CE056F-4779-4593-949A-42478734F747": {},
    }

    vector_types = ["importance", "target"]
    preferences = {
        "GL": {
            "Quick and Easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Single mealkit": "37CE056F-4779-4593-949A-42478734F747",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
            "Roede mealkit": "DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA",
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
        },
        "AMK": {
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
            "Quick and Easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
        },
        "LMK": {
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Quick and easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
        },
        "RT": {
            "Vegetarian": "6A494593-2931-4269-80EE-470D38F04796",
            "Chef's Favorite": "C94BCC7E-C023-40CE-81E0-C34DA3D79545",
            "Quick and easy": "C28F210B-427E-45FA-9150-D6344CAE669B",
            "Family Friendly": "B172864F-D58E-4395-B182-26C6A1F1C746",
            "Low calorie": "FD661CAD-7F45-4D02-A36E-12720D5C16CA",
        },
    }

    features = list(BasketFeatures.compile().request_all.needed_requests[0].all_features)
    data_points: list[dict] = []

    for company_name, comp_id in companies.items():
        for vector_type in vector_types:
            for concept_name, concept_id in preferences[company_name].items():
                default_concept_values = default_values[concept_id]

                row: dict = {
                    # Select either importance or target
                    # From the defaults, either set it to 0.0
                    feature.name: getattr(
                        default_concept_values.get(feature.name, FeatureImportance(0.0, 0.0)), vector_type
                    )
                    for feature in features
                }
                row["concept_id"] = concept_id
                row["concept_name"] = concept_name
                row["vector_type"] = vector_type
                row["company_id"] = comp_id
                data_points.append(row)

    return pl.DataFrame(data_points).lazy()


PreselectorErrorStructure = BasketFeatures.with_schema(
    name="preselector_error_vector", entities={}, source=InMemorySource.empty()
)

PredefinedVectors = BasketFeatures.with_schema(
    name="predefined_vectors",
    source=CustomMethodDataSource.from_load(generate_attribut_definition_vectors),
    materialized_source=materialized_data.parquet_at("predefined_vectors.parquet"),
    entities=dict(
        concept_id=String(), company_id=String(), vector_type=String().accepted_values(["importance", "target"])
    ),
    additional_features=dict(concept_name=String()),
    copy_default_values=True,
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
        copy_default_values=True,
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
