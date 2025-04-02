"Contains the 'model' of the preselector - if it can be called that."

import logging
from math import log2
from typing import Annotated

import numpy as np
import polars as pl
from aligned import ContractStore
from aligned.request.retrieval_request import RetrievalRequest
from aligned.retrieval_job import RetrievalJob
from data_contracts.preselector.basket_features import (
    BasketFeatures,
    PreselectorTags,
)
from data_contracts.recipe import (
    MealkitRecipeSimilarity,
    RecipeEmbedding,
    RecipeFeatures,
)

logger = logging.getLogger(__name__)


def explain_selection(
    error_expression: pl.Expr,
    target_vector: pl.Series,
    potential_vectors: pl.DataFrame,
    importance_vector: pl.Series,
    columns: list[str],
    explain_based_on_current: pl.DataFrame,
    exclude_column: str = "basket_id",
) -> None:
    "Tries to provide clues to why a recipes was chosen"

    import streamlit as st

    unimportant_columns = []
    for index, is_unimportant in enumerate((importance_vector == 0).to_list()):
        if is_unimportant:
            unimportant_columns.append(columns[index])

    top_vectors = (
        potential_vectors.select(pl.exclude(exclude_column))
        .select(columns)
        .transpose()
        .lazy()
        .select(error_expression)
        .collect()
        .transpose()
        .rename(lambda col: columns[int(col.split("_")[1])])
        .with_columns(total_error=pl.sum_horizontal(columns), recipe_id=potential_vectors[exclude_column])
        .sort("total_error", descending=False)
    )

    explanation = (
        top_vectors.select(columns)
        .transpose()
        .select(
            pl.all()
            - (
                (explain_based_on_current.select(columns).transpose().to_series() - target_vector)
                * importance_vector
                * 10
            )
            # Need to fill with 0 to avoid a nan sum
            # Which would lead to picking the first recipe in the list
            .fill_null(0)
            .fill_nan(0)
            .pow(2)
        )
        .transpose()
        .rename(lambda col: columns[int(col.split("_")[1])])
        .with_columns(change_in_error=pl.sum_horizontal(columns), recipe_id=top_vectors["recipe_id"])
        .select(pl.exclude(unimportant_columns))
    )
    st.write(explanation)

    st.write("Main reason for recipe")
    st.write(
        explanation.head(1)
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


def select_next_vector(
    target_vector: pl.Series,
    potential_vectors: pl.DataFrame,
    importance_vector: pl.Series,
    columns: list[str],
    exclude_column: str = "basket_id",
    rename_column: str = "recipe_id",
    explain_based_on_current: pl.DataFrame | None = None,
    error_column: str | None = None,
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
        error_column (str | None): The column where we return all the errors for each dim

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
        explain_selection(
            error_expression,
            target_vector,
            potential_vectors,
            importance_vector,
            columns,
            explain_based_on_current,
            exclude_column,
        )

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
                pl.struct(pl.col(columns)).alias(error_column),
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
        distance = (
            potential_vectors.with_columns(distance=distance["column_0"]).sort("distance", descending=False).limit(1)
        )

        return distance.select(
            pl.exclude(["distance", exclude_column]),
            pl.col(exclude_column).alias(rename_column),
        )


async def compute_basket(
    df: pl.DataFrame,
    aggregations: RetrievalRequest,
    simliarity_computation: RetrievalRequest,
    mealkit_embedding: list[float] | None,
    recipe_embeddings: Annotated[pl.DataFrame, RecipeEmbedding],
) -> pl.DataFrame:
    """
    Computes the basket features for a group of recipes.

    e.g. recipe a and b -> 1 chicken, 0.2 similarity, 0.5 CoF, etc.
    """
    job = RetrievalJob.from_polars_df(df, [aggregations])
    agg_values = await job.aggregate(aggregations).to_polars()

    if mealkit_embedding is None:
        return agg_values.with_columns(intra_week_similarity=pl.lit(0))

    with_mealkit = recipe_embeddings.select(
        [pl.col("recipe_id"), pl.col("embedding"), pl.lit(mealkit_embedding).alias("mealkit_embedding")]
    )

    # Need to manually loop through the derived features
    # As there is a bug where polars crashes for some reason
    # May need to upgrade the major version
    for derive in simliarity_computation.derived_features:
        output = await derive.transformation.transform_polars(with_mealkit.lazy(), derive.name, ContractStore.empty())
        if isinstance(output, pl.LazyFrame):
            with_mealkit = output.collect()
        else:
            with_mealkit = with_mealkit.with_columns(output.alias(derive.name))

    similarity = with_mealkit.select(
        [
            pl.col("recipe_id"),
            # Normalize [0, 1] as all features will be in this range.
            ((pl.col("similarity") + 1) / 2).alias("intra_week_similarity"),
        ]
    )
    return agg_values.join(similarity, left_on="basket_id", right_on="recipe_id")


def update_nudge_with_recipes(final_combination: pl.DataFrame, raw_recipe_nudge: pl.DataFrame) -> pl.DataFrame:
    """
    Creates a new dataframe that enables us to compute where we end up if we choose recipe x.
    """

    # Using numpy in order to vectorize the code and improve the performance
    repeated_recipes = np.repeat(raw_recipe_nudge["recipe_id"].to_numpy(), final_combination.height)
    return (
        pl.concat([final_combination.select(pl.exclude("basket_id"))] * raw_recipe_nudge.height, how="vertical")
        .hstack([pl.Series(values=repeated_recipes, name="basket_id")])
        .vstack(
            raw_recipe_nudge,
        )
        .sort(["basket_id", "recipe_id"])
    )


async def setup_starting_state(
    recipes_to_choose_from: pl.DataFrame,
    number_of_recipes: int,
    preselected_recipe_ids: list[int] | None,
    aggregations: RetrievalRequest,
    simliarity_computation: RetrievalRequest,
    mealkit_embedding: list[float] | None,
    recipe_embeddings: Annotated[pl.DataFrame, RecipeEmbedding],
) -> tuple[int, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Setup the state of the preselector.

    This is especially important in cases where we have a few recipe ids that needs to be in the basekt.
    E.g. due to negative preferences
    """

    async def default_response() -> tuple[int, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        n_recipes_to_add = number_of_recipes
        recipe_nudge = (
            await compute_basket(
                recipes_to_choose_from.with_columns(basket_id=pl.col("recipe_id")),
                aggregations,
                simliarity_computation,
                mealkit_embedding,
                recipe_embeddings,
            )
        ).sort("basket_id", descending=False)

        return n_recipes_to_add, recipe_nudge, pl.DataFrame(), recipes_to_choose_from

    if preselected_recipe_ids is None or not preselected_recipe_ids:
        return await default_response()

    final_combination = recipes_to_choose_from.filter(pl.col("main_recipe_id").is_in(preselected_recipe_ids))
    if final_combination.is_empty():
        return await default_response()

    raw_recipe_nudge = recipes_to_choose_from.filter(
        pl.col("main_recipe_id").is_in(preselected_recipe_ids).not_()
    ).with_columns(basket_id=pl.col("recipe_id"))

    raw_recipe_nudge = update_nudge_with_recipes(final_combination, raw_recipe_nudge)

    recipe_nudge = (
        await compute_basket(
            raw_recipe_nudge, aggregations, simliarity_computation, mealkit_embedding, recipe_embeddings
        )
    ).sort("basket_id", descending=False)

    recipes_to_choose_from = recipes_to_choose_from.filter(
        pl.col("main_recipe_id").is_in(preselected_recipe_ids).not_()
    )
    if len(preselected_recipe_ids) != final_combination.height:
        found_recipe_ids = final_combination["main_recipe_id"].to_list()
        missing_ids = set(preselected_recipe_ids) - set(found_recipe_ids)
        logger.error(f"We might be missing some features for ({missing_ids})")

    n_recipes_to_add = number_of_recipes - final_combination.height
    return n_recipes_to_add, recipe_nudge, final_combination, recipes_to_choose_from


async def find_best_combination(
    target_combination_values: pl.DataFrame,
    importance_vector: pl.DataFrame,
    available_recipes: Annotated[pl.DataFrame, RecipeFeatures],
    recipe_embeddings: Annotated[pl.DataFrame, RecipeEmbedding],
    number_of_recipes: int,
    preselected_recipe_ids: list[int] | None = None,
    should_explain: bool = True,
) -> tuple[list[int], Annotated[dict[str, float], "Soft preference error"]]:
    """
    Finds the best combination based on the underlying error logic.

    This will therefore, add one by one recipe to the mealkit until we fulfill the requirements.
    """
    final_combination = pl.DataFrame()

    columns = target_combination_values.columns

    recipes_to_choose_from = available_recipes

    simliarity_computation = MealkitRecipeSimilarity.query().request
    basket_aggregation = BasketFeatures.query()
    basket_computations = basket_aggregation.request
    mealkit_embedding = None

    current_vector: pl.DataFrame | None = None

    if should_explain:
        current_vector = pl.DataFrame({col: [0] for col in columns})

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

    n_recipes_to_add, recipe_nudge, final_combination, recipes_to_choose_from = await setup_starting_state(
        recipes_to_choose_from,
        number_of_recipes,
        preselected_recipe_ids,
        basket_computations,
        simliarity_computation,
        mealkit_embedding,
        recipe_embeddings,
    )

    for index in range(n_recipes_to_add):
        if recipe_nudge.is_empty():
            logger.info("Had no more recipes to add, so will return")
            return (final_combination["recipe_id"].to_list(), dict())

        if should_explain:
            import streamlit as st

            st.write("New candidate vectors")
            st.write(recipe_nudge)

        # For a five recipe selection on the first run
        # log(1.2) => 0.26
        binary_weight = log2(1 + (final_combination.height + 1) / number_of_recipes)

        next_vector = select_next_vector(
            mean_target_vector,
            recipe_nudge,
            feature_importance.with_columns([pl.col(feat) * binary_weight for feat in binary_features])
            .transpose()
            .to_series(),
            columns,
            explain_based_on_current=current_vector,
            error_column=error_column if index == n_recipes_to_add - 1 else None,
        )

        selected_recipe_id = next_vector[0, "recipe_id"]
        if should_explain:
            current_vector = next_vector

        next_recipe = recipes_to_choose_from.filter(pl.col("recipe_id") == selected_recipe_id)
        final_combination = final_combination.vstack(next_recipe)

        recipes_to_choose_from = recipes_to_choose_from.filter(
            pl.col("recipe_id") != selected_recipe_id,
        )

        raw_recipe_nudge = recipes_to_choose_from.with_columns(
            pl.col("recipe_id").alias("basket_id"),
        )

        if final_combination.height > 1:
            mean_emb = np.mean(
                recipe_embeddings.filter(pl.col("main_recipe_id").is_in(final_combination["main_recipe_id"]))[
                    "embedding"
                ].to_numpy(),
                axis=0,
            )
            mealkit_embedding = (mean_emb / np.linalg.norm(mean_emb)).tolist()
        else:
            mealkit_embedding = recipe_embeddings.filter(
                pl.col("main_recipe_id").is_in(final_combination["main_recipe_id"])
            )["embedding"].to_list()[0]

        raw_recipe_nudge = update_nudge_with_recipes(final_combination, raw_recipe_nudge)

        # Sorting in order to get deterministic results
        recipe_nudge = (
            await compute_basket(
                raw_recipe_nudge, basket_computations, simliarity_computation, mealkit_embedding, recipe_embeddings
            )
        ).sort(
            "basket_id",
            descending=False,
        )

    if should_explain and current_vector is not None:
        import streamlit as st

        st.write("Resulting vector")
        st.write(current_vector)

        st.write("Weighted result vector")
        st.write(
            (current_vector.select(columns).transpose() * importance_vector.select(columns).transpose())
            .transpose()
            .rename(lambda col: columns[int(col.split("_")[1])])
        )

        st.write(final_combination.to_pandas())

    return (final_combination["main_recipe_id"].to_list(), next_vector[error_column].to_list()[0])
