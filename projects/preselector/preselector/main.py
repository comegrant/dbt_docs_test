import logging
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from math import log2
from typing import Annotated

import numpy as np
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
from data_contracts.orders import WeeksSinceRecipe
from data_contracts.preselector.basket_features import (
    BasketFeatures,
    ImportanceVector,
    PreselectorTags,
    TargetVectors,
    VariationTags,
)
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek
from data_contracts.recipe import (
    MealkitRecipeSimilarity,
    RecipeEmbedding,
    RecipeFeatures,
    RecipeMainIngredientCategory,
    RecipeNegativePreferences,
)
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
from preselector.schemas.output import PreselectorWeekOutput

logger = logging.getLogger(__name__)


def select_next_vector(
    target_vector: pl.Series,
    potential_vectors: pl.DataFrame,
    importance_vector: pl.Series,
    columns: list[str],
    exclude_column: str = "basket_id",
    rename_column: str = "recipe_id",
    explain_based_on_current: pl.DataFrame | None = None,
    error_column: str | None = None
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
        error_column (str | None): The colum where we return all the errors for each dim

    Returns:
        pl.DataFrame: The vector that is closes to the target.
    """

    error_expression = (
        ((pl.all() - target_vector) * 10 * importance_vector)
        # Need to fill with 0 to avoid a nan sum
        # Which would lead to picking the first recipe in the list
        .fill_null(0)
        .fill_nan(0)
        .pow(2)
    )

    if explain_based_on_current is not None:
        import streamlit as st

        unimportant_columns = []
        for index, is_unimportant in enumerate((importance_vector == 0).to_list()):
            if is_unimportant:
                unimportant_columns.append(
                    columns[index]
                )

        top_vectors = (
            potential_vectors.select(pl.exclude(exclude_column))
            .select(columns)
            .transpose()
            .lazy()
            .select(error_expression)
            .collect()
            .transpose()
            .rename(lambda col: columns[int(col.split("_")[1])])
            .with_columns(
                total_error=pl.sum_horizontal(columns),
                recipe_id=potential_vectors[exclude_column]
            )
            .sort("total_error", descending=False)
        )

        explaination = (
            top_vectors.select(columns)
            .transpose()
            .select(
                pl.all() -
                (
                    (
                        explain_based_on_current.select(columns).transpose().to_series() - target_vector
                    ) * importance_vector * 10
                )
                # Need to fill with 0 to avoid a nan sum
                # Which would lead to picking the first recipe in the list
                .fill_null(0)
                .fill_nan(0)
                .pow(2)
            )
            .transpose()
            .rename(lambda col: columns[int(col.split("_")[1])])
            .with_columns(
                change_in_error=pl.sum_horizontal(columns),
                recipe_id=top_vectors["recipe_id"]
            )
            .select(pl.exclude(unimportant_columns))
        )
        st.write(explaination)

        st.write("Main reason for recipe")
        st.write(
            explaination.head(1)
                .select(pl.exclude(unimportant_columns))
                .transpose(header_name="feature", include_header=True)
                .sort("column_0", descending=False)
                .filter(pl.col("feature").is_in(["change_in_error"]).not_())
                .head(10)
        )

        st.write("Current biggest errors")
        st.write(
            top_vectors.head(1)
                .select(pl.exclude(unimportant_columns))
                .transpose(header_name="feature", include_header=True)
                .filter(pl.col("feature").is_in(["recipe_id", "total_error"]).not_())
                .sort("column_0", descending=True)
                .head(10)

        )
        st.write("Current basket vector")
        st.write(top_vectors.head(1))

    if error_column:
        return (
            potential_vectors.select(pl.exclude(exclude_column))
            .select(columns)
            .transpose()
            .lazy()
            .select(error_expression)
            .collect()
            .transpose()
            .rename(lambda col: columns[int(col.split("_")[1])])
            .with_columns(
                pl.sum_horizontal(columns).alias("total_error"),
                potential_vectors[exclude_column].alias(rename_column),
                pl.struct(pl.col(columns)).alias(error_column)
            )
            .sort("total_error", descending=False)
            .head(1)
        )
    else:
        distance = (
            potential_vectors.select(pl.exclude(exclude_column))
            .select(columns)
            .transpose()
            .lazy()
            .select(error_expression.sum())
            .collect()
            .transpose()
        )
        distance = potential_vectors.with_columns(
            distance=distance["column_0"]
        ).sort("distance", descending=False).limit(1)

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
    recipe_embeddings: Annotated[pl.DataFrame, RecipeEmbedding],
    number_of_recipes: int,
    preselected_recipe_ids: list[int] | None = None,
    should_explain: bool = True,
) -> tuple[list[int], Annotated[dict[str, float], "Soft preference error"]]:

    final_combination = pl.DataFrame()

    columns = target_combination_values.columns

    recipes_to_choose_from = available_recipes

    simliarity_computation = MealkitRecipeSimilarity.query().request
    basket_aggregation = BasketFeatures.query()
    basket_computations = basket_aggregation.request
    mealkit_embedding = None

    async def compute_basket(df: pl.DataFrame) -> pl.DataFrame:
        """
        Computes the basket features for a group of recipes.

        e.g. recipe a and b -> 1 chicken, 0.2 similary, 0.5 CoF, etc.
        """
        job = RetrivalJob.from_polars_df(df, [basket_computations])
        aggregations = await job.aggregate(basket_computations).to_polars()

        if mealkit_embedding is None:
            return aggregations.with_columns(
                inter_week_similarity=pl.lit(0)
            )

        with_mealkit = recipe_embeddings.select([
            pl.col("recipe_id"),
            pl.col("embedding"),
            pl.lit(mealkit_embedding).alias("mealkit_embedding")
        ])

        # Need to manually loop through the derived features
        # As there is a bug where polars crashes for some reason
        # May need to upgrade the major version
        for derive in simliarity_computation.derived_features:
            output = (await derive.transformation.transform_polars(
                with_mealkit.lazy(), derive.name, ContractStore.empty()
            ))
            assert isinstance(output, pl.LazyFrame)
            with_mealkit = output.collect()

        similarity = with_mealkit.select([
            pl.col("recipe_id"),
            # Normalize [0, 1] as all features will be in this range.
            ((pl.col("similarity") + 1) / 2).alias("inter_week_similarity")
        ])
        return aggregations.join(
            similarity, left_on="basket_id", right_on="recipe_id"
        )

    def update_nudge_with_recipes(
        final_combination: pl.DataFrame,
        raw_recipe_nudge: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Creates a new dataframe that enables us to compute where we end up if we choose recipe x.
        """

        # Using numpy in order to vectorize the code and imporve the performance
        repeated_recipes = np.repeat(raw_recipe_nudge["recipe_id"].to_numpy(), final_combination.height)
        return pl.concat(
            [final_combination.select(pl.exclude("basket_id"))] * raw_recipe_nudge.height,
            how="vertical"
        ).hstack(
            [pl.Series(values=repeated_recipes, name="basket_id")]
        ).vstack(
            raw_recipe_nudge,
        ).sort(["basket_id", "recipe_id"])


    async def setup_starting_state(
        recipes_to_choose_from: pl.DataFrame,
    ) -> tuple[int, pl.DataFrame]:
        async def default_response() -> tuple[int, pl.DataFrame]:
            n_recipes_to_add = number_of_recipes
            recipe_nudge = (
                await compute_basket(
                    recipes_to_choose_from.with_columns(basket_id=pl.col("recipe_id")),
                )
            ).sort("basket_id", descending=False)

            return n_recipes_to_add, recipe_nudge

        if preselected_recipe_ids is None or not preselected_recipe_ids:
            return await default_response()

        final_combination = recipes_to_choose_from.filter(
            pl.col("recipe_id").is_in(preselected_recipe_ids)
        )

        if not final_combination.is_empty():
            return await default_response()

        raw_recipe_nudge = recipes_to_choose_from.filter(
            pl.col("recipe_id").is_in(preselected_recipe_ids).not_()
        ).with_columns(basket_id=pl.col("recipe_id"))

        raw_recipe_nudge = update_nudge_with_recipes(final_combination, raw_recipe_nudge)

        recipe_nudge = (
            await compute_basket(raw_recipe_nudge)
        ).sort("basket_id", descending=False)

        recipes_to_choose_from = recipes_to_choose_from.filter(
            pl.col("recipe_id").is_in(preselected_recipe_ids).not_()
        )
        if len(preselected_recipe_ids) != final_combination.height:
            found_recipe_ids = final_combination["recipe_id"].to_list()
            missing_ids = set(preselected_recipe_ids) - set(found_recipe_ids)
            logger.error(f"We might be missing some features for ({missing_ids})")

        n_recipes_to_add = number_of_recipes - final_combination.height
        return n_recipes_to_add, recipe_nudge


    current_vector: pl.DataFrame | None = None

    if should_explain:
        current_vector = pl.DataFrame({
            col: [0] for col in columns
        })

    mean_target_vector = target_combination_values.select(columns).transpose().to_series()
    feature_importance = importance_vector.select(columns)

    binary_features = [
        feat.name
        for feat in basket_computations.all_features
        if feat.tags and PreselectorTags.binary_metric in feat.tags
    ]

    # Just to fix a type error
    next_vector = pl.DataFrame()
    error_column = "dim_errors"

    n_recipes_to_add, recipe_nudge = await setup_starting_state(recipes_to_choose_from)

    for index in range(n_recipes_to_add):

        # For a five recipe selection on the first run
        # log(1.2) => 0.26

        binary_weight = log2(1 + (final_combination.height + 1) / number_of_recipes)

        if recipe_nudge.is_empty():
            return (final_combination["recipe_id"].to_list(), dict())

        if should_explain:
            import streamlit as st
            st.write("New candidate vectors")
            st.write(recipe_nudge)

        next_vector = select_next_vector(
            mean_target_vector,
            recipe_nudge,
            feature_importance.with_columns([
                pl.col(feat) * binary_weight
                for feat in binary_features
            ]).transpose().to_series(),
            columns,
            explain_based_on_current=current_vector,
            error_column=error_column if index == n_recipes_to_add - 1 else None
        )

        selected_recipe_id = next_vector[0, "recipe_id"]
        if should_explain:
            current_vector = next_vector

        final_combination = final_combination.vstack(
            recipes_to_choose_from.filter(pl.col("recipe_id") == selected_recipe_id),
        )

        recipes_to_choose_from = recipes_to_choose_from.filter(
            pl.col("recipe_id") != selected_recipe_id,
        )

        raw_recipe_nudge = recipes_to_choose_from.with_columns(
            pl.col("recipe_id").alias("basket_id"),
        )

        if final_combination.height > 1:
            mean_emb = np.mean(recipe_embeddings.filter(
                pl.col("main_recipe_id").is_in(final_combination["main_recipe_id"])
            )["embedding"].to_numpy(), axis=0)
            mealkit_embedding = (mean_emb / np.linalg.norm(mean_emb)).tolist()
        else:
            mealkit_embedding = recipe_embeddings.filter(
                pl.col("main_recipe_id").is_in(final_combination["main_recipe_id"])
            )["embedding"].to_list()[0]

        raw_recipe_nudge = update_nudge_with_recipes(final_combination, raw_recipe_nudge)

        # Sorting in order to get deterministic results
        recipe_nudge = (await compute_basket(raw_recipe_nudge)).sort(
            "basket_id",
            descending=False,
        )

    if should_explain and current_vector is not None:
        import streamlit as st

        st.write("Resulting vector")
        st.write(current_vector)

        st.write("Weighted result vector")
        st.write(
            (
                current_vector.select(columns).transpose() * importance_vector.select(columns).transpose()
            )
            .transpose()
            .rename(lambda col: columns[int(col.split("_")[1])])
        )

        st.write(final_combination.to_pandas())

    return (
        final_combination["main_recipe_id"].to_list(),
        next_vector[error_column].to_list()[0]
    )


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
    preds = await store.model("rec_engine").all_predictions().filter(
        (pl.col("agreement_id") == agreement_id)
        & (pl.col("year") == year)
        & (pl.col("week") == week)
    ).to_lazy_polars()
    return (
        preds.sort("predicted_at", descending=True)
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
    combined_targets: list[pl.DataFrame] = []

    importances = predefined_vector.filter(
        pl.col("vector_type") == "importance"
    ).with_columns(
        pl.col(feat) / pl.sum_horizontal(features) for feat in features
    )

    # Setting the new target based on the following formula when there are multiple concepts
    # target_f = (target_1 * importance_1 + target_2 * importance_2) / sum(importance_f)
    for concept_id in concept_ids:
        importance = importances.filter(
            pl.col("concept_id") == concept_id
        ).select(features)

        target = predefined_vector.filter(
            (pl.col("vector_type") == "target")
            & (pl.col("concept_id") == concept_id)
        ).select(features)

        combined_targets.append(
            (importance.transpose() * target.transpose()).transpose()
        )

    if not combined_targets:
        raise ValueError(
            f"Unable to find any target or importance vector for the concept ids: {concept_ids} in company {company_id}"
        )

    target_vector = (
        (pl.concat(combined_targets).sum().transpose() / importances.select(features).sum().transpose()).fill_nan(0)
    ).transpose().rename(lambda col: features[int(col.split("_")[1])])

    importance_vector = importances.select(combined_feature)

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
    with duration("load-cof-min-max-in-week"):
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

    assert min_cof, (
        f"Missing min cof for {request.company_id}, year: {year_week.year}, week: {year_week.week}, "
        f"{request.portion_size} portions. Therefore, can not normalize the cof target"
    )
    assert max_cof, "Missing max cof. Therefore, can not normalize the cof target"
    assert min_cof != max_cof, (
        f"Min and Max CoF are the same. This most likely means something is wrong for {year_week}"
    )
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
        if not features:
            return True
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

    if not tags:
        return importance, target

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


def handle_calorie_concept(
    target: pl.DataFrame, importance: pl.DataFrame, concept_ids: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:

    low_cal_id = "FD661CAD-7F45-4D02-A36E-12720D5C16CA"
    vegetarian_id = "6A494593-2931-4269-80EE-470D38F04796"
    roede_id = "DF81FF77-B4C4-4FC1-A135-AB7B0704D1FA"

    # Roede
    if concept_ids == [low_cal_id]:
        return (
            target.with_columns(
                is_low_calorie=pl.lit(1)
            ),
            importance.with_columns(
                is_low_calorie=pl.lit(1)
            )
        )
    elif roede_id in concept_ids:
        # Roede can choose negative preferences, so we will not select a pre-defined mealkit
        # But rather find the most optimal selection
        # As a result will we weight the roede features above everything else.
        # But if there are no left, then they will start to get other types of dishes
        target = target.with_columns(
            is_roede_percentage=pl.lit(1)
        )
        importance = importance.with_columns(
            is_roede_percentage=pl.lit(1)
        )
        return target, importance
    else:
        target = target.with_columns(
            is_roede_percentage=pl.lit(0)
        )
        importance = importance.with_columns(
            is_roede_percentage=pl.lit(1)
        )

    if vegetarian_id not in concept_ids and low_cal_id not in concept_ids:
        return (
            target.with_columns(
                is_low_calorie=pl.lit(0)
            ),
            importance.with_columns(
                is_low_calorie=pl.lit(0.5)
            )
        )
    else:
        return target, importance



async def historical_preselector_vector(
    agreement_id: int,
    request: GenerateMealkitRequest,
    store: ContractStore,
) -> tuple[pl.DataFrame, pl.DataFrame, Annotated[bool, "If the vectors is based on historical data"]]:

    async def inject_importance_and_target(
        importance: pl.DataFrame, target: pl.DataFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        from data_contracts.preselector.basket_features import InjectedFeatures

        importance_static = (await InjectedFeatures.process_input({
            "mean_cost_of_food": [0.25],
            "mean_rank": [0.02],
            "mean_ordered_ago": [0.3],
            "inter_week_similarity": [0.07]
        }).drop_invalid().to_polars()).to_dicts()[0]

        target_static = (await InjectedFeatures.process_input({
            # Mean cost target will be set in the generate week
            "mean_cost_of_food": [0],
            "mean_rank": [0],
            # aka max
            "mean_ordered_ago": [0],
            "inter_week_similarity": [0]
        }).drop_invalid().to_polars()).to_dicts()[0]

        importance, target = (
            importance.with_columns([
                pl.lit(value).alias(key)
                for key, value in importance_static.items()
            ]),
            target.with_columns([
                pl.lit(value).alias(key)
                for key, value in target_static.items()
            ])
        )

        return (
            importance.with_columns([
                pl.col(feat) / pl.sum_horizontal(vector_features)
                for feat in importance.columns
            ]),
            target
        )


    vector_features = [
        feat.name for feat in store.feature_view(TargetVectors).request.features if "float" in feat.dtype.name
    ]

    logger.debug(f"No history found, using default values {request.concept_preference_ids}")

    company_id = request.company_id
    concept_ids = [concept_id.upper() for concept_id in request.concept_preference_ids]

    with duration("load-concept-definitions"):
        default_importance, default_target = await importance_vector_for_concept(concept_ids, store, company_id)

    default_importance = default_importance.with_columns(
        pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features
    )

    if request.has_data_processing_consent:
        with duration("load-importance-vector"):
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
        default_importance, default_target = potentially_add_variation(default_importance, default_target)
        default_importance = default_importance.with_columns(
            pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features
        )

        default_target, default_importance = handle_calorie_concept(
            default_target, default_importance, concept_ids
        )
        default_importance, default_target = await inject_importance_and_target(
            importance=default_importance, target=default_target
        )
        return default_target, default_importance, False

    with duration("load-target-vector"):
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
    user_vector_weight = 2
    attribute_vector_weight = 1
    vector_sum = user_vector_weight + attribute_vector_weight

    with duration("find-attributes-to-overwrite-in-vector"):
        overwrite_columns: list[str] = [
            key
            for key, value in default_importance.to_dicts()[0].items()
            if isinstance(value, float) and value > 0
        ]
        other_features = list(set(vector_features) - set(overwrite_columns))


    with duration("combine-importance-vectors"):
        combined_importance = (
            default_importance.select(pl.col(vector_features) * attribute_vector_weight).vstack(
                user_importance.select(pl.col(vector_features) * user_vector_weight)
            ).sum().select(pl.all() / vector_sum)
        )

        combined_target = pl.concat([
            default_target.select(overwrite_columns),
            user_target.select(other_features)
        ], how="horizontal")

    combined_importance, combined_target = await inject_importance_and_target(
        importance=combined_importance, target=combined_target
    )
    combined_target, combined_importance = handle_calorie_concept(
        combined_target, combined_importance, concept_ids
    )


    user_importance = combined_importance

    return (
        combined_target,
        combined_importance,
        True,
    )


@contextmanager
def duration(metric: str) -> Generator[None, None, None]:
    import os
    from time import monotonic

    from datadog.dogstatsd.base import statsd

    if statsd.host is None:
        yield
    else:
        metric_name = metric.lower().replace(" ", "-")
        metric_name = f"preselector.{metric_name}_time.histogram"
        start_time = monotonic()
        yield
        tags = None
        # Shit solution but but
        if "service_bus_request_topic_name" in os.environ:
            topic = os.environ["service_bus_request_topic_name".upper()]
            tags = [f"topic:{topic}"]
        statsd.histogram(metric_name, monotonic() - start_time, tags=tags)

mealkit_cache = {}


def cached_output(
    concepts: list[str],
    year_week: YearWeek,
    portion_size: int,
    number_of_recipes: int,
    taste_preference_ids: list[str],
    company_id: str,
    ordered_in_week: dict[int, int] | None
) -> PreselectorYearWeekResponse | None:
    key = f"{year_week.year}-{year_week.week}-{concepts}-{portion_size}-{number_of_recipes}-{taste_preference_ids}-{company_id}-{ordered_in_week}" # noqa: E501
    return mealkit_cache.get(key)


def set_cache(
    result: PreselectorYearWeekResponse,
    concepts: list[str],
    year_week: YearWeek,
    portion_size: int,
    number_of_recipes: int,
    taste_preference_ids: list[str],
    company_id: str,
    ordered_in_week: dict[int, int] | None
) -> None:
    key = f"{year_week.year}-{year_week.week}-{concepts}-{portion_size}-{number_of_recipes}-{taste_preference_ids}-{company_id}-{ordered_in_week}" # noqa: E501
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
            taste_preferences=self.request.taste_preferences,
            company_id=self.request.company_id,
            has_data_processing_consent=self.request.has_data_processing_consent,
            number_of_recipes=self.request.number_of_recipes,
            portion_size=self.request.portion_size,
        )

async def cost_of_food_target_for(
    request: GenerateMealkitRequest,
    store: ContractStore
) -> Annotated[pl.DataFrame, CostOfFoodPerMenuWeek]:

    with duration("load-mealkit-cof-target"):
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
    request: GenerateMealkitRequest, store: ContractStore, should_explain: bool = False
) -> PreselectorResult:
    results: list[PreselectorYearWeekResponse] = []


    subscription_variation = sorted(request.concept_preference_ids)
    sorted_taste_pref = sorted(request.taste_preference_ids)

    if not request.has_data_processing_consent and request.agreement_id == 0 and len(request.compute_for) == 1:
        # Super fast cache for
        cached_value = cached_output(
            subscription_variation,
            request.compute_for[0],
            request.portion_size,
            request.number_of_recipes,
            sorted_taste_pref,
            request.company_id,
            request.ordered_weeks_ago
        )
        if cached_value:
            return PreselectorResult(
                success=[cached_value],
                failures=[],
                request=request,
                model_version=model_version(),
                generated_at=datetime.now(tz=timezone.utc),
            )


    cost_of_food = await cost_of_food_target_for(request, store)

    assert cost_of_food.height == len(request.compute_for), (
        f"Got {cost_of_food.height} CoF targets expected {len(request.compute_for)}"
    )

    with duration("construct-vector"):
        (
            target_vector,
            importance_vector,
            contains_history,
        ) = await historical_preselector_vector(
            agreement_id=request.agreement_id,
            request=request,
            store=store,
        )

    if should_explain:
        logger.info("Explaining data through streamlit")
        import streamlit as st

        st.write("Importance vector")
        st.write(importance_vector)

        st.write("Target vector")
        st.write(target_vector)


    failed_weeks: list[PreselectorFailure] = []

    # main_recipe_id: year_week it was last ordered
    generated_recipe_ids: dict[int, int] = request.ordered_weeks_ago or {}

    for yearweek in request.compute_for:
        year = yearweek.year
        week = yearweek.week

        if (year * 100 + week) in generated_recipe_ids:
            logger.info(f"Skipping for {year} {week}")
            continue

        logger.debug(f"Running for {year} {week}")

        if not contains_history:
            cached_value = cached_output(
                subscription_variation,
                yearweek,
                request.portion_size,
                request.number_of_recipes,
                sorted_taste_pref,
                request.company_id,
                request.ordered_weeks_ago
            )
            if cached_value:
                results.append(cached_value)
                continue

        cof_target = cost_of_food.filter(
            pl.col("year") == year, pl.col("week") == week
        )

        if cof_target.height != 1:
            failed_weeks.append(
                PreselectorFailure(
                    error_message=(
                        f"Expected only one cof target for a week got {cof_target.height} for year week {yearweek}."
                        "This is usually a sign that the menu is missing."
                    ),
                    error_code=2,
                    year=yearweek.year,
                    week=yearweek.week
                )
            )
            continue

        cof_target_value = cof_target["cost_of_food_target_per_recipe"].to_list()[0]

        try:
            target_vector = await normalize_cost(
                year_week=yearweek,
                target_cost_of_food=cof_target_value,
                request=request,
                vector=target_vector,
                store=store,
            )
        except AssertionError as e:
            failed_weeks.append(
                PreselectorFailure(
                    error_message=(
                        str(e) + "This is usually a sign that the menu is missing."
                    ),
                    error_code=3,
                    year=yearweek.year,
                    week=yearweek.week
                )
            )
            continue

        logger.debug("Loading menu")
        with duration("load-menu"):
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

        if request.has_data_processing_consent:
            logger.debug("Loading recommendations")
            with duration("loading-recommendations"):
                recommendations = await load_recommendations(
                    agreement_id=request.agreement_id, year=year, week=week, store=store
                )
        else:
            recommendations = pl.DataFrame()

        could_be_ww = False

        # Only for Linas, Low Calorie, and if it is only week 51 that is computed.
        # Using processing concent as a proxy for "has taken quiz"
        if not request.has_data_processing_consent and (
            request.company_id == "6A2D0B60-84D6-4830-9945-58D518D27AC2"
        ) and (
            request.concept_preference_ids == ["FD661CAD-7F45-4D02-A36E-12720D5C16CA"]
        ) and (
            request.compute_for == [YearWeek(year=2024, week=51)]
        ):
            could_be_ww = True

        logger.debug("Running preselector")
        try:
            with duration("running-preselector"):
                output = await run_preselector(
                    request,
                    menu,
                    recommendations,
                    target_vector=target_vector,
                    importance_vector=importance_vector,
                    store=store,
                    selected_recipes=generated_recipe_ids,
                    could_be_weight_watchers=could_be_ww,
                    should_explain=should_explain
                )
                selected_recipe_ids = output.main_recipe_ids
        except (AssertionError, ValueError) as e:
            logger.exception(e)
            failed_weeks.append(
                PreselectorFailure(
                    error_message=str(e),
                    error_code=500,
                    year=yearweek.year,
                    week=yearweek.week
                )
            )
            continue

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
            variation_ids=variation_ids,
            main_recipe_ids=selected_recipe_ids,
            compliancy=output.compliancy,
            target_cost_of_food_per_recipe=cof_target_value,
            ordered_weeks_ago=generated_recipe_ids,
            error_vector=output.error_vector
        )

        for main_recipe_id in selected_recipe_ids:
            generated_recipe_ids[main_recipe_id] = year * 100 + week

        if not contains_history:
            set_cache(
                result,
                subscription_variation,
                yearweek,
                request.portion_size,
                request.number_of_recipes,
                sorted_taste_pref,
                request.company_id,
                request.ordered_weeks_ago
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
) -> tuple[pl.DataFrame, PreselectorPreferenceCompliancy, list[int] | None]:

    if not customer.taste_preferences:
        return recipes, PreselectorPreferenceCompliancy.all_compliant, None

    recipes_to_use = await filter_out_recipes_based_on_preference(
        recipes,
        portion_size=customer.portion_size,
        taste_preference_ids=customer.taste_preference_ids,
        store=store
    )

    if recipes_to_use.height >= customer.number_of_recipes:
        return recipes_to_use, PreselectorPreferenceCompliancy.all_compliant, None

    preselected_recipes = recipes_to_use["recipe_id"].to_list()

    recipes_to_use = await filter_out_recipes_based_on_preference(
        recipes,
        portion_size=customer.portion_size,
        taste_preference_ids=[
            pref.preference_id
            for pref in customer.taste_preferences
            if pref.is_allergy
        ],
        store=store
    )

    if recipes_to_use.height >= customer.number_of_recipes:
        return recipes_to_use, PreselectorPreferenceCompliancy.allergies_only_compliant, preselected_recipes

    preselected_recipes = recipes_to_use["recipe_id"].to_list()
    return recipes, PreselectorPreferenceCompliancy.non_preference_compliant, preselected_recipes


async def compute_weeks_ago(
    company_id: str,
    agreement_id: int,
    year_week: int,
    main_recipe_ids: list[int],
    store: ContractStore
) -> pl.DataFrame:

    number_of_recipes = len(main_recipe_ids)
    req = store.feature_view(WeeksSinceRecipe).request
    derived_feature_names = sorted([
        feat.name for feat in req.derived_features
    ])

    job = store.feature_view(WeeksSinceRecipe).features_for({
        "agreement_id": [agreement_id] * number_of_recipes,
        "company_id": [company_id] * number_of_recipes,
        # Setting the from_year_week so it will be used when computing
        "from_year_week": [year_week] * number_of_recipes,
        "main_recipe_id": main_recipe_ids
    }).transform_polars(
        # Is currently a bug where this will not be compute
        # So need to remove it and then compute it again
        lambda df: df.select(
            pl.exclude(derived_feature_names)
        ) if derived_feature_names[0] in df.columns else df
    ).derive_features()

    return  (
        await job.to_polars()
    ).cast({"main_recipe_id": pl.Int32})


async def add_ordered_since_feature(
    customer: GenerateMealkitRequest,
    store: ContractStore,
    recipe_features: pl.DataFrame,
    year_week: int,
    selected_recipes: dict[int, int]
) -> pl.DataFrame:
    new_recipe_ids = recipe_features.filter(
        pl.col("main_recipe_id").is_in(selected_recipes.keys()).not_()
    )["main_recipe_id"]

    selected_recipe_computation: pl.DataFrame | None = None
    weeks_since_recipe: pl.DataFrame | None = None

    if selected_recipes:
        manual_data = {
            "main_recipe_id": list(selected_recipes.keys()),
            "last_order_year_week": list(selected_recipes.values()),
            "company_id": [customer.company_id] * len(selected_recipes),
            "agreement_id": [customer.agreement_id] * len(selected_recipes),
            "from_year_week": [year_week] * len(selected_recipes)
        }
        selected_recipe_computation = (await store.feature_view(WeeksSinceRecipe)
            .process_input(manual_data)
            .to_polars()
        ).cast({"main_recipe_id": pl.Int32})

    if customer.agreement_id != 0:
        weeks_since_recipe = await compute_weeks_ago(
            company_id=customer.company_id,
            agreement_id=customer.agreement_id,
            year_week=year_week,
            main_recipe_ids=new_recipe_ids.to_list(),
            store=store
        )

    return_columns = ["main_recipe_id", "ordered_weeks_ago"]
    default_value = 0

    if selected_recipe_computation is None and weeks_since_recipe is None:
        return recipe_features.with_columns(ordered_weeks_ago=pl.lit(default_value))
    elif selected_recipe_computation is not None and weeks_since_recipe is not None:
        return recipe_features.cast({"main_recipe_id": pl.Int32}).join(
            weeks_since_recipe.select(return_columns).vstack(
                selected_recipe_computation.select(return_columns)
            ),
            on="main_recipe_id",
            how="left"
        ).with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
    elif selected_recipe_computation is not None:
        return recipe_features.cast({"main_recipe_id": pl.Int32}).join(
            selected_recipe_computation.select(return_columns),
            on="main_recipe_id",
            how="left"
        ).with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
    elif weeks_since_recipe is not None:
        return recipe_features.cast({"main_recipe_id": pl.Int32}).join(
            weeks_since_recipe.select(return_columns),
            on="main_recipe_id",
            how="left"
        ).with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
    else:
        raise ValueError("Should never happen")



async def run_preselector(
    customer: GenerateMealkitRequest,
    available_recipes: Annotated[pl.DataFrame, Menu],
    recommendations: Annotated[pl.DataFrame, RecommendatedDish],
    target_vector: pl.DataFrame,
    importance_vector: pl.DataFrame,
    store: ContractStore,
    selected_recipes: dict[int, int],
    could_be_weight_watchers: bool,
    should_explain: bool = False,
) -> PreselectorWeekOutput:
    """
    Generates a combination of recipes that best fit a personalised target and importance vector.

    Arguments:
        customer (GenerateMealkitRequest): The request defining contraints about the mealkit
        available_recipes (pl.DataFrame): The Available recipes that can be chosen
        recommendations (pl.DataFrame): The recommendations for a user
        target_vector (pl.DataFrame): The target vector to hit
        importance_vector (pl.DataFrame): The importance of each feature in the target vector
        store (ContractStore): The definition of available features
    """
    assert not available_recipes.is_empty(), "No recipes to select from"
    assert not target_vector.is_empty(), "No target vector to compare with"
    assert target_vector.height == importance_vector.height, "Target and importance vector must have the same length"

    compliance = PreselectorPreferenceCompliancy.all_compliant

    recipes = available_recipes

    logger.debug(f"Filtering based on portion size: {recipes.height}")
    recipes = recipes.filter(
        pl.col("variation_portions").cast(pl.Int32) == customer.portion_size,
    )
    logger.debug(f"Filtering based on portion size done: {recipes.height}")

    if recipes.is_empty():
        return PreselectorWeekOutput([], compliance, {})

    # Singlekassen
    if customer.concept_preference_ids == ["37CE056F-4779-4593-949A-42478734F747"]:
        return PreselectorWeekOutput(
            recipes["main_recipe_id"].sample(customer.number_of_recipes).to_list(),
            compliance,
            {}
        )


    year = recipes["menu_year"].max()
    week = recipes["menu_week"].max()

    assert year is not None, f"Found no recipes for year {year} and week {week}"
    assert week is not None

    with duration("load-normalized-features"):
        normalized_recipe_features = await compute_normalized_features(
            recipes.with_columns(
                year=pl.lit(year),
                week=pl.lit(week),
                portion_size=customer.portion_size,
                company_id=pl.lit(customer.company_id)
            ),
            store=store,
        )

    if should_explain:
        import streamlit as st
        st.write("Raw Recipe Features")
        st.write(normalized_recipe_features)

    if could_be_weight_watchers:
        # Only return weight watchers recipes up to week 51 in Linas
        filtered = normalized_recipe_features.filter(
            pl.col("is_weight_watchers")
        )
        if filtered.height >= customer.number_of_recipes:
            return PreselectorWeekOutput(
                filtered.sample(customer.number_of_recipes)["main_recipe_id"].to_list(),
                compliance,
                {}
            )
        else:
            available_ww_recipes = filtered["main_recipe_id"].to_list()
            logger.error(
                f"Should have returned a mealkit for WW, but it did not. Available recipes:{available_ww_recipes}"
            )
    else:
        filtered = normalized_recipe_features.filter(
            pl.col("is_adams_signature").is_not()
            & pl.col("is_cheep").is_not()
            & pl.col("is_weight_watchers").is_not()
            & pl.col("is_slow_grown_chicken").is_not()
        ).select(
            pl.exclude(["is_adams_signature", "is_cheep"]),
        )

    filtered, compliance, preselected_recipes = await filter_on_preferences(
        customer, filtered, store
    )
    logger.debug(f"Loading preselector recipe features: {recipes.height}")

    if filtered.height >= customer.number_of_recipes:
        normalized_recipe_features = filtered

    with duration("load-main-ingredient-catagory"):
        normalized_recipe_features = await store.feature_view(
            RecipeMainIngredientCategory
        ).features_for(
            normalized_recipe_features
        ).filter(
            # Only removing those without carbo.
            # Those without protein id can be vegetarian
            pl.col("main_carbohydrate_category_id").is_not_null()
        ).with_subfeatures().to_polars()

    with duration("load-recipe-cost"):
        normalized_recipe_features = (
            await store.feature_view("recipe_cost")
            .select({"recipe_cost_whole_units"})
            .features_for(normalized_recipe_features)
            .with_subfeatures()
            .to_polars()
        )

    recipe_features = normalized_recipe_features

    if recipe_features.height < customer.number_of_recipes:
        recipes_of_interest = (
            set(recipes["recipe_id"].to_list())
            - set(recipe_features["recipe_id"].to_list())
        )
        logger.error(
            f"Number of recipes are less then expected {recipe_features.height}. "
            f"Most likely due to missing features in recipes: ({recipes_of_interest}) "
            f"In portion size {customer.portion_size} - {year}, {week}"
        )
        return PreselectorWeekOutput(
            recipes.sample(customer.number_of_recipes)["main_recipe_id"].to_list(),
            compliance,
            {}
        )

    if should_explain:
        import streamlit as st
        st.write("Recipe Candidates")
        st.write(recipe_features)

    if recipe_features.is_empty():
        raise ValueError(
            "Found no recipes to select from. "
            "Please let the data team know about this so we can fix it. "
            f"Had initially {recipes.height} recipes, and {recipe_features.height} recipes with features"
        )

    if not recommendations.is_empty():
        with duration("add-recommendations-data"):
            with_rank = recipes.cast({
                "recipe_id": pl.Int32
            }).join(
                recommendations.select(["product_id", "order_rank"]),
                on="product_id",
                how="left"
            ).unique("recipe_id")

            recipe_features = recipe_features.select(pl.exclude("order_rank")).join(
                with_rank.select(["recipe_id", "order_rank"]),
                on="recipe_id",
                how="left"
            ).with_columns(
                pl.col("order_rank").fill_null(pl.lit(recipe_features.height / 2))
            ).with_columns(
                order_rank=pl.col("order_rank").log() / pl.col("order_rank").log().max().clip(lower_bound=1)
            )
            logger.debug(
                f"Filtering based on recommendations done: {recipe_features.height}",
            )

    logger.debug(
        f"Selecting the best combination based on {recipe_features.height} recipes.",
    )

    if recipe_features.height <= customer.number_of_recipes:
        logger.error(f"Too few recipes to run the preselector for agreement: {customer.agreement_id}")
        return PreselectorWeekOutput(
            recipes.filter(pl.col("recipe_id").is_in(recipe_features["recipe_id"]))["main_recipe_id"].to_list(),
            compliance,
            {}
        )

    with duration("compute-ordered-since"):
        recipe_features = await add_ordered_since_feature(
            customer,
            store,
            recipe_features,
            year_week=year * 100 + week, # type: ignore
            selected_recipes=selected_recipes
        )

    with duration("load-recipe-embeddings"):
        recipe_embeddings = await store.model(RecipeEmbedding).predictions_for(
            recipe_features.select([
                "main_recipe_id", "company_id"
            ])
        ).select(["embedding"]).to_polars()

        recipe_embeddings = recipe_embeddings.join(
            recipe_features.cast({ "main_recipe_id": pl.Int32 }).select(["recipe_id", "main_recipe_id"]),
            on="main_recipe_id"
        )

    if recipe_embeddings.height != recipe_features.height:
        missing_recipes = set(
            recipe_features["main_recipe_id"].to_list()
        ) - set(recipe_embeddings["main_recipe_id"].to_list())
        logger.error(
            f"We are missing some embeddings for main recipe ids: {missing_recipes}"
        )

    assert not recipe_features.is_empty(), (
        f"Found no features something is very wrong for {customer.agreement_id}, {year}, {week}"
    )

    with duration("find-best-combination"):
        best_recipe_ids, error = await find_best_combination(
            target_vector,
            importance_vector,
            recipe_features.unique(["main_recipe_id"]),
            recipe_embeddings=recipe_embeddings,
            number_of_recipes=customer.number_of_recipes,
            preselected_recipe_ids=preselected_recipes,
            should_explain=should_explain,
        )

    if should_explain and error is not None:
        import streamlit as st
        st.write("Error Vector:")
        st.write(error)

    return PreselectorWeekOutput(
        main_recipe_ids=best_recipe_ids,
        compliancy=compliance,
        error_vector=error
    )
