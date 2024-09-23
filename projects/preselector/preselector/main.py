import logging
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated

import polars as pl
from aligned import (
    ContractStore,
    FileSource,
    Int32,
    String,
    feature_view,
)
from aligned.retrival_job import RetrivalJob
from data_contracts.mealkits import OneSubMealkits
from data_contracts.preselector.basket_features import (
    BasketFeatures,
    ImportanceVector,
    TargetVectors,
    VariationTags,
)
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek
from data_contracts.recipe import RecipeFeatures, RecipeMainIngredientCategory
from data_contracts.recommendations.recommendations import RecommendatedDish

from preselector.contracts.compare_boxes import compute_normalized_features
from preselector.data.models.customer import (
    PreselectorFailedResponse,
    PreselectorFailure,
    PreselectorPreferenceCompliancy,
    PreselectorSuccessfulResponse,
    PreselectorYearWeekResponse,
)
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek

logger = logging.getLogger(__name__)


def select_next_vector(
    target_vector: pl.Series,
    potential_vectors: pl.DataFrame,
    importance_vector: pl.Series,
    columns: list[str],
    exclude_column: str = "basket_id",
    rename_column: str = "recipe_id",
) -> pl.DataFrame:
    """
    Selects the vector that is closest to the target vector

    ```python
    new_vector = select_next_vector(
        target_vector=pl.Series(values=[0, 1, 1]),
        potential_vectors=pl.DataFrame(
            data=[
                [1, 0, 0, 1],
                [0, 0, 1, 2],
                [0, 1, 0, 3],
                [1, 1, 1, 4],
            ],
            schema=["a", "b", "c", "basket_id"],
            orient="row",
        ),
        importance_vector=pl.Series(values=[1, 1, 1]),
        columns=["a", "b", "c"],
        exclude_column="basket_id",
        rename_column="recipe_id",
    )

    print(new_vector.to_numpy().to_list())
    ```
    >>> [[0, 0, 1, 2]]

    Args:
        target_vector (pl.Series): The ideal vector to reach
        potential_vectors (pl.DataFrame): The values we end up at if we select a recipe
        importance_vector (pl.Series): How important are the different features
        columns (list[str]): The feature located at the vector indexes
        exclude_column (str): The column to exclude in the computations. E.g. IDs
        rename_column (str): What to name the exclude column in the response

    Returns:
        pl.DataFrame: The vector that is closes to the target.
    """
    distance = (
        potential_vectors.select(pl.exclude(exclude_column))
        .select(columns)
        .transpose()
        .lazy()
        .select(
            ((pl.all() - target_vector) * importance_vector * 10)
            # Need to fill with 0 to avoid a nan sum
            # Which would lead to picking the first recipe in the list
            .fill_null(0)
            .fill_nan(0)
            .pow(2)
            # Using sum, as it is faster than mean and lead to the same result
            .sum()
        )
        .collect()
        .transpose()
    )


    # import streamlit as st
    # st.write(
    #     potential_vectors.select(pl.exclude(exclude_column))
    #     .select(columns)
    #     .transpose()
    #     .lazy()
    #     .select(
    #         ((pl.all() - target_vector) * importance_vector * 10)
    #         # Need to fill with 0 to avoid a nan sum
    #         # Which would lead to picking the first recipe in the list
    #         .fill_null(0)
    #         .fill_nan(0)
    #         .pow(2)
    #         # Using sum, as it is faster than mean and lead to the same result
    #     )
    #     .collect()
    #     .transpose()
    #     .rename(lambda col: columns[int(col.split("_")[1])])
    #     .to_pandas()
    # )


    distance = potential_vectors.with_columns(distance=distance["column_0"]).sort("distance", descending=False).limit(1)

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
        .fill_nan(0)
        .transpose()
        .to_series()
    )


async def find_best_combination(
    target_combination_values: pl.DataFrame,
    importance_vector: pl.DataFrame,
    available_recipes: Annotated[pl.DataFrame, RecipeFeatures],
    number_of_recipes: int,
) -> tuple[list[int], Annotated[dict, "Soft preference error"]]:
    final_combination = pl.DataFrame()

    columns = target_combination_values.columns

    recipes_to_choose_from = available_recipes

    basket_aggregation = BasketFeatures.query()
    basket_computations = basket_aggregation.request

    async def compute_basket(df: pl.DataFrame) -> pl.DataFrame:
        job = RetrivalJob.from_polars_df(df, [basket_computations])
        return await job.aggregate(basket_computations).to_polars()

    # Sorting in order to get deterministic results
    recipe_nudge = (
        await compute_basket(
            recipes_to_choose_from.with_columns(basket_id=pl.col("recipe_id")),
        )
    ).sort("basket_id", descending=False)

    mean_target_vector = target_combination_values.select(columns).transpose().to_series()
    feature_importance = importance_vector.select(columns).transpose().to_series()

    for _ in range(number_of_recipes):
        if recipe_nudge.is_empty():
            return (final_combination["recipe_id"].to_list(), dict())

        next_vector = select_next_vector(
            mean_target_vector,
            recipe_nudge,
            feature_importance,
            columns,
        )

        selected_recipe_id = next_vector[0, "recipe_id"]
        final_combination = final_combination.vstack(
            recipes_to_choose_from.filter(pl.col("recipe_id") == selected_recipe_id),
        )

        recipes_to_choose_from = recipes_to_choose_from.filter(
            pl.col("recipe_id") != selected_recipe_id,
        )

        raw_recipe_nudge = recipes_to_choose_from.with_columns(
            pl.col("recipe_id").alias("basket_id"),
        )

        for recipe_id in raw_recipe_nudge["recipe_id"].to_list():
            raw_recipe_nudge = pl.concat(
                [
                    raw_recipe_nudge,
                    final_combination.with_columns(pl.lit(recipe_id).alias("basket_id")),
                ],
                how="vertical_relaxed",
            )

        # Sorting in order to get deterministic results
        recipe_nudge = (await compute_basket(raw_recipe_nudge)).sort(
            "basket_id",
            descending=False,
        )

    # import streamlit as st
    # st.write(final_combination.to_pandas())

    return (final_combination["recipe_id"].to_list(), dict())


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


async def load_menu_for(company_id: str, year: int, week: int, store: ContractStore) -> pl.DataFrame:
    menus = await store.feature_view("preselector_year_week_menu").all().to_lazy_polars()
    menu = menus.filter(
        pl.col("menu_year") == year,
        pl.col("menu_week") == week,
        pl.col("company_id").str.to_lowercase() == company_id.lower(),
    )
    return menu.collect()


async def load_recommendations(
    agreement_id: int,
    year: int,
    week: int,
    store: ContractStore,
) -> pl.DataFrame:
    preds = await store.model("rec_engine").all_predictions().to_lazy_polars()
    return (
        preds.filter(
            pl.col("agreement_id") == agreement_id,
            pl.col("year") == year,
            pl.col("week") == week,
        )
        .sort("predicted_at", descending=True)
        .unique(["agreement_id", "week", "year", "product_id"], keep="first")
    ).collect()


async def importance_vector_for_concept(
    concept_ids: list[str], store: ContractStore, company_id: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    features = BasketFeatures.query().request.request_result.feature_columns

    entities: dict[str, list] = {
        "concept_id": [],
        "company_id": [],
        "vector_type": []
    }

    for concept_id in concept_ids:
        entities["concept_id"].extend([concept_id, concept_id])
        entities["company_id"].extend([company_id, company_id])
        entities["vector_type"].extend(["importance", "target"])


    predefined_vector = (
        await store.feature_view("predefined_vectors")
        .features_for(entities)
        .drop_invalid()
        .to_polars()
    )

    combined_feature = [
        pl.col(feature).where(pl.col(feature) > 0).mean().fill_nan(0).fill_null(0) for feature in features
    ]
    importance_vector = predefined_vector.filter(pl.col("vector_type") == "importance").select(combined_feature)
    target_vector = predefined_vector.filter(pl.col("vector_type") == "target").select([
        pl.col(feature).sum() for feature in features
    ])

    assert (
        not importance_vector.is_empty()
    ), f"Predefined importance vector is missing for concept {concept_ids} company: {company_id}"
    assert (
        not target_vector.is_empty()
    ), f"Predefined target vector is missing for concept {concept_ids} company: {company_id}"
    return (importance_vector, target_vector)


async def normalize_cost(
    year_week: YearWeek,
    target_cost_of_food: float,
    request: GenerateMealkitRequest,
    vector: pl.DataFrame,
    store: ContractStore,
) -> pl.DataFrame:
    with duration("Loading CoF min max normalization values"):
        cost_of_food_normalization = (
            await store.feature_view("menu_week_recipe_stats")
            .select({"min_cost_of_food", "max_cost_of_food"})
            .features_for(
                {
                    "company_id": [request.company_id],
                    "menu_week": [year_week.week],
                    "menu_year": [year_week.year],
                    "portion_size": [request.portion_size],
                }
            )
            .to_polars()
        )

    min_cof = cost_of_food_normalization["min_cost_of_food"].to_list()[0]
    max_cof = cost_of_food_normalization["max_cost_of_food"].to_list()[0]

    logger.debug(f"Min CoF {min_cof} max: {max_cof}")


    assert min_cof, "Missing min cof. Therefore, can not normalize the cof target"
    assert max_cof, "Missing max cof. Therefore, can not normalize the cof target"
    cost_of_food_value = (target_cost_of_food - min_cof) / (max_cof - min_cof)

    logger.debug(f"Normalized value from {target_cost_of_food} to {cost_of_food_value}")

    return vector.with_columns(mean_cost_of_food=pl.lit(cost_of_food_value))


@dataclass
class FeatureImportance:
    target: float
    importance: float

def potentially_add_variation(importance: pl.DataFrame, target: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:

    features = list(BasketFeatures.compile().request_all.needed_requests[0].all_features)

    def contains_features(features: list[str], importance: pl.DataFrame) -> bool:
        max_imp_value = importance.select([
            pl.col(feature) for feature in features
        ]).max_horizontal().to_list()[0]
        return max_imp_value != 0


    def fill_when_missing(
        default_values: dict[str, float],
        importance: pl.DataFrame,
        target: pl.DataFrame,
        fixed_imporatnce: float
    ) -> tuple[pl.DataFrame, pl.DataFrame]:

        importance = importance.with_columns([
            pl.lit(fixed_imporatnce).alias(key)
            for key  in default_values
        ])
        target = target.with_columns([
            pl.lit(val).alias(key)
            for key, val in default_values.items()
        ])

        return importance, target


    potential_tags = {
        VariationTags.protein: 0.2,
        VariationTags.carbohydrate: 0.2,
        VariationTags.quality: 0.8,
        VariationTags.equal_dishes: 0.0
    }

    tags = dict()

    for tag, value in potential_tags.items():
        feat = [
            feature.name
            for feature in features
            if feature.tags and tag in feature.tags
        ]

        if not contains_features(feat, importance):
            tags[tag] = value

    if not tags:
        return importance, target

    # These total sum 0.5 will lead to
    # Attributes = 2/3
    # Variation = 1/3
    # Since they should be summed to 1
    total_sum = 0.25 / len(tags)


    for tag, value in tags.items():
        vector = {
            feature.name: value
            for feature in features
            if feature.tags and tag in feature.tags
        }

        importance, target = fill_when_missing(
            vector,
            importance,
            target,
            total_sum / len(vector)
        )

    return importance, target

async def historical_preselector_vector(
    agreement_id: int,
    request: GenerateMealkitRequest,
    store: ContractStore,
) -> tuple[pl.DataFrame, pl.DataFrame, Annotated[bool, "If the vectors is based on historical data"]]:

    vector_features = [
        feat.name for feat in store.feature_view(TargetVectors).request.features if "float" in feat.dtype.name
    ]

    logger.debug(f"No history found, using default values {request.concept_preference_ids}")

    company_id = request.company_id
    concept_ids = [concept_id.upper() for concept_id in request.concept_preference_ids]

    with duration("Loading concept definition"):
        default_importance, default_target = await importance_vector_for_concept(concept_ids, store, company_id)

    default_importance = default_importance.with_columns(
        pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features
    )
    default_importance, default_target = potentially_add_variation(default_importance, default_target)
    default_importance = default_importance.with_columns(
        pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features
    )
    default_importance = default_importance.with_columns(mean_cost_of_food=pl.lit(0.25))

    if request.has_data_processing_consent:
        with duration("Loading importance vector"):
            user_importance = (
                await store.feature_view(ImportanceVector)
                .features_for(
                    {
                        "agreement_id": [agreement_id],
                    }
                )
                .drop_invalid()
                .to_polars()
            )
    else:
        user_importance = pl.DataFrame()

    if user_importance.is_empty():
        return default_target, default_importance, False

    with duration("Loading target vector"):
        user_target = (
            await store.feature_view(TargetVectors)
            .features_for(
                {
                    "agreement_id": [agreement_id],
                }
            )
            .drop_invalid()
            .to_polars()
        )

    # Stacking the user vector two times to weight that 2 / 3
    combined_importance = default_importance.select(vector_features).vstack(
        user_importance.vstack(user_importance).select(vector_features)
    ).mean().with_columns(
        mean_cost_of_food=pl.lit(0.25)
    )
    combined_target = default_target.select(vector_features).vstack(
        user_target.vstack(user_target).select(vector_features)
    ).mean()

    user_importance = combined_importance

    return (
        combined_target.select(vector_features),
        combined_importance.select(vector_features),
        True,
    )


@contextmanager
def duration(log: str, should_log: bool = False) -> Generator[None, None, None]:
    # if tracemalloc.is_tracing():
    #     now, max_memory = tracemalloc.get_traced_memory()
    #     megabytes = 2**20
    #     logger.info(
    #         f"Starting with (current, max) bytes ({now / megabytes} MB, {max_memory / megabytes} MB)",
    #     )

    if should_log:
        start = time.monotonic()
        yield
        end = time.monotonic()
        logger.info(f"{log} took {end - start} seconds")
    else:
        yield
    # if tracemalloc.is_tracing():
    #     now, max_memory = tracemalloc.get_traced_memory()
    #     megabytes = 2**20
    #     logger.info(
    #         f"Completed with (current, max) bytes ({now / megabytes} MB, {max_memory / megabytes} MB)",
    #     )


mealkit_cache = {}


def cached_output(
    concepts: list[str],
    year_week: YearWeek,
    portion_size: int,
    number_of_recipes: int,
    taste_preference_ids: list[str],
    company_id: str
) -> PreselectorYearWeekResponse | None:
    key = f"{year_week.year}-{year_week.week}-{concepts}-{portion_size}-{number_of_recipes}-{taste_preference_ids}-{company_id}" # noqa: E501
    return mealkit_cache.get(key)


def set_cache(
    result: PreselectorYearWeekResponse,
    concepts: list[str],
    year_week: YearWeek,
    portion_size: int,
    number_of_recipes: int,
    taste_preference_ids: list[str],
    company_id: str
) -> None:
    key = f"{year_week.year}-{year_week.week}-{concepts}-{portion_size}-{number_of_recipes}-{taste_preference_ids}-{company_id}" # noqa: E501
    mealkit_cache[key] = result


def model_version() -> str:
    import os

    default = "testing"
    version = os.getenv("GIT_COMMIT_HASH", default)
    if not version:
        return default
    else:
        return version


@dataclass
class PreselectorResult:
    success: list[PreselectorYearWeekResponse]
    failures: list[PreselectorFailure]
    request: GenerateMealkitRequest
    model_version: str
    generated_at: datetime

    def failed_responses(self) -> list[PreselectorFailedResponse]:
        return [
            PreselectorFailedResponse(
                error_message=failure.error_message,
                error_code=failure.error_code,
                request=self.request
            )
            for failure in self.failures
        ]


    def success_response(self) -> PreselectorSuccessfulResponse | None:
        if not self.success:
            return None

        return PreselectorSuccessfulResponse(
            agreement_id=self.request.agreement_id,
            year_weeks=self.success,
            override_deviation=self.request.override_deviation,
            model_version=self.model_version,
            generated_at=self.generated_at,
            correlation_id=self.request.correlation_id,
            concept_preference_ids=self.request.concept_preference_ids,
            taste_preferences=self.request.taste_preferences
        )

async def cost_of_food_target_for(
    request: GenerateMealkitRequest,
    store: ContractStore
) -> Annotated[pl.DataFrame, CostOfFoodPerMenuWeek]:

    with duration("Loading mealkit CoF target"):
        cof_entities = {
            "company_id": [request.company_id] * len(request.compute_for),
            "number_of_recipes": [request.number_of_recipes] * len(request.compute_for),
            "number_of_portions": [request.portion_size] * len(request.compute_for),
            "year": [ over.year for over in request.compute_for ],
            "week": [ over.week for over in request.compute_for ]
        }

        cost_of_food = await (store.feature_view(CostOfFoodPerMenuWeek)
            .select({"cost_of_food_target_per_recipe"})
            .features_for(cof_entities)
            .to_polars()
        )

        missing = cost_of_food.filter(pl.col("cost_of_food_target_per_recipe").is_null())

        if missing.is_empty():
            return cost_of_food

        default_cof = await (
            store.feature_view(OneSubMealkits)
            .select({"cost_of_food_target_per_recipe"})
            .features_for(
                {
                    "company_id": [request.company_id],
                    "number_of_recipes": [request.number_of_recipes],
                    "number_of_portions": [request.portion_size],
                }
            )
            .to_polars()
        )

        defaults = missing.join(
            default_cof,
            on=["company_id", "number_of_recipes", "number_of_portions"],
            suffix="_right"
        )
        assert defaults.height == missing.height, "Filling inn default values did not go as expected"

        cost_of_food = cost_of_food.vstack(
            defaults.with_columns(
                pl.col("cost_of_food_target_per_recipe").fill_null(pl.col("cost_of_food_target_per_recipe_right"))
            ).select(cost_of_food.columns)
        ).filter(pl.col("cost_of_food_target_per_recipe").is_not_null())

    return cost_of_food


async def run_preselector_for_request(
    request: GenerateMealkitRequest, store: ContractStore
) -> PreselectorResult:
    results: list[PreselectorYearWeekResponse] = []

    all_selected_main_recipe_ids = request.quarentine_main_recipe_ids.copy()

    cost_of_food = await cost_of_food_target_for(request, store)

    assert cost_of_food.height == len(request.compute_for), (
        f"Got {cost_of_food.height} CoF targets expected {len(request.compute_for)}"
    )

    with duration("Computing preselector vector"):
        (
            target_vector,
            importance_vector,
            contains_history,
        ) = await historical_preselector_vector(
            agreement_id=request.agreement_id,
            request=request,
            store=store,
        )

    subscription_variation = sorted(request.concept_preference_ids)
    sorted_taste_pref = sorted(request.taste_preference_ids)

    failed_weeks: list[PreselectorFailure] = []

    for yearweek in request.compute_for:
        year = yearweek.year
        week = yearweek.week

        logger.debug(f"Running for {year} {week}")

        if not contains_history:
            cached_value = cached_output(
                subscription_variation,
                yearweek,
                request.portion_size,
                request.number_of_recipes,
                sorted_taste_pref,
                request.company_id
            )
            if cached_value:
                results.append(cached_value)
                continue

        cof_target = cost_of_food.filter(
            pl.col("year") == year, pl.col("week") == week
        )
        assert cof_target.height == 1, (
            f"Expected only one cof target for a week got {cof_target.height} for year week {yearweek}"
        )

        cof_target_value = cof_target["cost_of_food_target_per_recipe"].to_list()[0]

        target_vector = await normalize_cost(
            year_week=yearweek,
            target_cost_of_food=cof_target_value,
            request=request,
            vector=target_vector,
            store=store,
        )

        logger.debug("Loading menu")
        with duration("Loading menu"):
            menu = await load_menu_for(request.company_id, year, week, store=store)
        logger.debug(f"Number of recipes {menu.height}")

        if menu.is_empty():
            failed_weeks.append(
                PreselectorFailure(
                    error_message=(
                        f"Found no menu for {year}-{week} and company {request.company_id}"
                    ),
                    error_code=1,
                    year=yearweek.year,
                    week=yearweek.week
                )
            )
            continue

        menu = menu.filter(~pl.col("main_recipe_id").is_in(all_selected_main_recipe_ids))
        logger.debug(f"Removed quarantined recipes. Left with {menu.height} recipes.")

        if request.has_data_processing_consent:
            logger.debug("Loading recommendations")
            with duration("Loading recommendations"):
                recommendations = await load_recommendations(
                    agreement_id=request.agreement_id, year=year, week=week, store=store
                )
        else:
            recommendations = pl.DataFrame()

        logger.debug("Running preselector")
        with duration("Running preselector", should_log=True):
            selected_recipe_ids, _ = await run_preselector(
                request,
                menu,
                recommendations,
                target_vector=target_vector,
                importance_vector=importance_vector,
                store=store,
            )

        if len(selected_recipe_ids) != request.number_of_recipes:
            failed_weeks.append(
                PreselectorFailure(
                    error_message=(
                        f"Only managed to find {len(selected_recipe_ids)} "
                        f"recipes out of {request.number_of_recipes} recipes."
                    ),
                    error_code=1,
                    year=yearweek.year,
                    week=yearweek.week
                )
            )
            continue

        all_selected_main_recipe_ids.extend(selected_recipe_ids)
        variation_ids = menu.filter(
            pl.col("main_recipe_id").is_in(selected_recipe_ids),
            pl.col("variation_portions") == request.portion_size,
        )["variation_id"].to_list()

        assert len(variation_ids) == request.number_of_recipes, (
            "Number of recipes and variation ids do not match. "
            "This is an internal error, which could be due to data an error"
        )

        result = PreselectorYearWeekResponse(
            year=year,
            week=week,
            portion_size=request.portion_size,
            variation_ids=variation_ids,
            main_recipe_ids=selected_recipe_ids,
            compliancy=PreselectorPreferenceCompliancy.all_complient,
            target_cost_of_food_per_recipe=cof_target_value
        )
        if not contains_history:
            set_cache(
                result,
                subscription_variation,
                yearweek,
                request.portion_size,
                request.number_of_recipes,
                sorted_taste_pref,
                request.company_id
            )

        results.append(result)

    return PreselectorResult(
        success=results,
        failures=failed_weeks,
        request=request,
        model_version=model_version(),
        generated_at=datetime.now(tz=timezone.utc),
    )

async def filter_out_recipes_based_on_preference(
    recipes: pl.DataFrame,
    portion_size: int,
    taste_preference_ids: list[str],
    store: ContractStore
) -> pl.DataFrame:
    """
    Filters out any recipes that conflict with a hard filter rule.
    Or also known as a taste preferences.

    Returns:
        pl.DataFrame: The recipes that do not conflict with the taste preferences
    """
    with duration("Loading recipe preferences"):
        preferences = (
            await store.feature_view("recipe_preferences")
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



async def run_preselector(
    customer: GenerateMealkitRequest,
    available_recipes: Annotated[pl.DataFrame, Menu],
    recommendations: Annotated[pl.DataFrame, RecommendatedDish],
    target_vector: pl.DataFrame,
    importance_vector: pl.DataFrame,
    store: ContractStore,
    select_top_n: int = 20,
    top_n_percent: float = 0.3,
) -> tuple[list[int], dict]:
    """
    Generates a combination of recipes that best fit a personalised target and importance vector.

    Arguments:
        customer (GenerateMealkitRequest): The request defining contraints about the mealkit
        available_recipes (pl.DataFrame): The Available recipes that can be chosen
        recommendations (pl.DataFrame): The recommendations for a user
        target_vector (pl.DataFrame): The target vector to hit
        importance_vector (pl.DataFrame): The importance of each feature in the target vector
        store (ContractStore): The definition of available features
        select_top_n: The recipes to choose from, if recommendations exist
        top_n_percent: A percentage to choose from, if recommendations exist
    """
    assert not available_recipes.is_empty(), "No recipes to select from"
    assert not target_vector.is_empty(), "No target vector to compare with"
    assert target_vector.height == importance_vector.height, "Target and importance vector must have the same length"

    recipes = available_recipes

    logger.debug(f"Filtering based on portion size: {recipes.height}")
    recipes = recipes.filter(
        pl.col("variation_portions").cast(pl.Int32) == customer.portion_size,
    )
    logger.debug(f"Filtering based on portion size done: {recipes.height}")

    if recipes.is_empty():
        return ([], {})

    if customer.taste_preference_ids:
        recipes = await filter_out_recipes_based_on_preference(
            recipes,
            portion_size=customer.portion_size,
            taste_preference_ids=customer.taste_preference_ids,
            store=store
        )
        logger.debug(f"Filtering based on taste preferences done: {recipes.height}")

    logger.debug(f"Loading preselector recipe features: {recipes.height}")

    year = recipes["menu_year"].max()
    week = recipes["menu_week"].max()

    with duration("Loading normalized recipe features"):
        normalized_recipe_features = await compute_normalized_features(
            recipes.with_columns(
                year=pl.lit(year),
                week=pl.lit(week),
                portion_size=customer.portion_size,
                company_id=pl.lit(customer.company_id)
            ),
            store=store,
        )

    with duration("Loading main ingredient category"):
        normalized_recipe_features = await store.feature_view(
            RecipeMainIngredientCategory.metadata.name
        ).features_for(
            normalized_recipe_features
        ).filter(
            # Only removing those without carbo.
            # Those without protein id can be vegetarian
            pl.col("main_carbohydrate_category_id").is_not_null()
        ).with_subfeatures().to_polars()

    with duration("Load recipe cost features"):
        normalized_recipe_features = (
            await store.feature_view("recipe_cost")
            .select({"recipe_cost_whole_units"})
            .features_for(normalized_recipe_features)
            .with_subfeatures()
            .to_polars()
        )

    with duration("Loading recipe features"):
        all_recipe_features = await (
            store.model("preselector")
            .features_for(
                recipes.with_columns(pl.lit(customer.portion_size).alias("portion_size")),
            )
            .to_polars()
        )

    logger.debug(f"Filtering out cheep and premium recipes: {all_recipe_features.height}")

    static_mealkits = ["37CE056F-4779-4593-949A-42478734F747"]

    if customer.concept_preference_ids != static_mealkits and customer.portion_size != 1:
        recipe_features = all_recipe_features.filter(
            pl.col("is_cheep").is_not(),
        ).select(
            pl.exclude(["is_cheep"]),
        )
        logger.debug(f"Filtered out cheep and premium recipes: {recipe_features.height}")
    else:
        recipe_features = all_recipe_features

    if recipe_features.is_empty():
        raise ValueError(
            "Found no recipes to select from. "
            "Please let the data team know about this so we can fix it. "
            f"Had initially {recipes.height} recipes, and {all_recipe_features.height} recipes with features"
        )

    select_top_n = min(
        max(
            customer.number_of_recipes * 4,
            int(recipe_features.height * top_n_percent),
        ),
        recipe_features.height,
    )

    if select_top_n < 1:
        raise ValueError("No recipes to select from. Please let the data team know about this so we can fix it.")

    if recommendations.height > select_top_n:
        with duration("Filtering on recs"):
            logger.debug(
                f"Selecting top {select_top_n} based on recommendations: {recipe_features.height}",
            )
            product_ids = recipes["product_id"].to_list()

            top_n_recipes = (
                recommendations.filter(pl.col("product_id").is_in(product_ids))
                .sort("order_rank", descending=False)
                .limit(select_top_n)
            )

            recipes = recipes.filter(
                pl.col("product_id").is_in(top_n_recipes["product_id"]),
            )
            recipe_features = recipe_features.filter(
                pl.col("recipe_id").is_in(recipes["recipe_id"]),
            )
            logger.debug(
                f"Filtering based on recommendations done: {recipe_features.height}",
            )

    logger.debug(
        f"Selecting the best combination based on {recipe_features.height} recipes.",
    )

    if recipe_features.height <= customer.number_of_recipes:
        logger.debug(f"Too few recipes to run the preselector for agreement: {customer.agreement_id}")
        return (
            recipes.filter(pl.col("recipe_id").is_in(recipe_features["recipe_id"]))["main_recipe_id"].to_list(),
            dict(),
        )

    with duration("Filtering based on available recipes"):
        recipes_to_choose_from = normalized_recipe_features.filter(
            pl.col("recipe_id").is_in(recipe_features["recipe_id"])
        )

    assert not normalized_recipe_features.is_empty()

    with duration("Finding best combination"):
        best_recipe_ids, error_metric = await find_best_combination(
            target_vector,
            importance_vector,
            recipes_to_choose_from,
            customer.number_of_recipes,
        )

    return (
        recipes.filter(pl.col("recipe_id").is_in(best_recipe_ids))["main_recipe_id"].to_list(),
        error_metric,
    )
