import logging
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
from data_contracts.attribute_scoring import AttributeScoring
from data_contracts.orders import WeeksSinceRecipe
from data_contracts.recipe import (
    RecipeEmbedding,
    RecipeMainIngredientCategory,
)
from data_contracts.recipe_vote import RecipeVote
from data_contracts.recommendations.recommendations import RecommendatedDish

from preselector.cache import cached_output, set_cache
from preselector.data.models.customer import (
    PreselectorFailedResponse,
    PreselectorFailure,
    PreselectorPreferenceCompliancy,
    PreselectorRecipeResponse,
    PreselectorSuccessfulResponse,
    PreselectorYearWeekResponse,
)
from preselector.filtering import filter_on_preferences, filter_out_unwanted_preselected_recipes
from preselector.model import find_best_combination
from preselector.model_version import model_version
from preselector.monitor import duration
from preselector.quarantining import add_ordered_since_feature
from preselector.schemas.batch_request import GenerateMealkitRequest
from preselector.schemas.output import PreselectorRecipe, PreselectorWeekOutput
from preselector.store import (
    compute_normalized_features,
    cost_of_food_target_for,
    load_menu_for,
    load_recommendations,
    normalize_cost,
)
from preselector.vectors import historical_preselector_vector

logger = logging.getLogger(__name__)


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
                error_message=failure.error_message, error_code=failure.error_code, request=self.request
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
            originated_at=self.request.originated_at,
        )


async def run_preselector_for_request(
    request: GenerateMealkitRequest, store: ContractStore, should_explain: bool = False
) -> PreselectorResult:
    """
    Runs the preselector for one generation request.

    Therefore, potentially generating multiple mealkits.


    """
    results: list[PreselectorYearWeekResponse] = []

    subscription_variation = sorted(request.concept_preference_ids)
    sorted_taste_pref = sorted(request.taste_preference_ids)

    if (
        not should_explain
        and not request.has_data_processing_consent
        and request.agreement_id == 0
        and len(request.compute_for) == 1
    ):
        # Super fast cache for
        cached_value = cached_output(
            subscription_variation,
            request.compute_for[0],
            request.portion_size,
            request.number_of_recipes,
            sorted_taste_pref,
            request.company_id,
            request.ordered_weeks_ago,
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

    assert cost_of_food.height == len(
        request.compute_for
    ), f"Got {cost_of_food.height} CoF targets expected {len(request.compute_for)}"

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

        columns = target_vector.columns
        vals = (
            pl.DataFrame({"columns": columns})
            .hstack(
                [
                    importance_vector.select(columns).transpose().rename({"column_0": "importance"}).to_series(),
                    target_vector.select(columns).transpose().rename({"column_0": "target"}).to_series(),
                ]
            )
            .sort("importance", descending=True)
        )

        st.write("Most important features")
        st.write(vals)

    failed_weeks: list[PreselectorFailure] = []

    # main_recipe_id: year_week it was last ordered
    generated_recipe_ids: dict[int, int] = {}

    if WeeksSinceRecipe.location.name in store.feature_views:
        quarantining_data = (
            await store.feature_view(WeeksSinceRecipe)
            .select({"last_order_year_week"})
            .filter(pl.col("agreement_id") == request.agreement_id)
            .to_polars()
        )
        if not quarantining_data.is_empty():
            for row in quarantining_data.iter_rows(named=True):
                generated_recipe_ids[row["main_recipe_id"]] = row["last_order_year_week"]

    if should_explain:
        import streamlit as st

        st.write("Quarantining data")
        st.write(generated_recipe_ids)

    if request.ordered_weeks_ago:
        for main_recipe_id, yearweek in request.ordered_weeks_ago.items():
            generated_recipe_ids[main_recipe_id] = yearweek

    for yearweek in request.compute_for:
        year = yearweek.year
        week = yearweek.week

        if (year * 100 + week) in generated_recipe_ids:
            logger.info(f"Skipping for {year} {week}")
            continue

        logger.debug(f"Running for {year} {week}")

        if not should_explain and not contains_history:
            cached_value = cached_output(
                subscription_variation,
                yearweek,
                request.portion_size,
                request.number_of_recipes,
                sorted_taste_pref,
                request.company_id,
                request.ordered_weeks_ago,
            )
            if cached_value:
                results.append(cached_value)
                continue

        cof_target = cost_of_food.filter(pl.col("year") == year, pl.col("week") == week)

        if cof_target.height != 1:
            failed_weeks.append(
                PreselectorFailure(
                    error_message=(
                        f"Expected only one cof target for a week got {cof_target.height} for year week {yearweek}."
                        "This is usually a sign that the menu is missing."
                    ),
                    error_code=2,
                    year=yearweek.year,
                    week=yearweek.week,
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
                    error_message=(str(e) + "This is usually a sign that the menu is missing."),
                    error_code=3,
                    year=yearweek.year,
                    week=yearweek.week,
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
                    error_message=(f"Found no menu for {year}-{week} and company {request.company_id}"),
                    error_code=1,
                    year=yearweek.year,
                    week=yearweek.week,
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
                    should_explain=should_explain,
                )
                selected_recipes = output.recipes
                selected_recipe_ids = output.main_recipe_ids
        except (AssertionError, ValueError) as e:
            logger.exception(e)
            failed_weeks.append(
                PreselectorFailure(error_message=str(e), error_code=500, year=yearweek.year, week=yearweek.week)
            )
            continue

        if len(selected_recipe_ids) != request.number_of_recipes:
            failed_weeks.append(
                PreselectorFailure(
                    error_message=(
                        f"Only managed to find {len(selected_recipe_ids)} "
                        f"recipes out of {request.number_of_recipes} recipes. "
                        f"Selected recipes: {output.recipes}"
                    ),
                    error_code=1,
                    year=yearweek.year,
                    week=yearweek.week,
                )
            )
            continue

        output_df = (
            menu.filter(
                pl.col("main_recipe_id").is_in(selected_recipe_ids),
                pl.col("variation_portions") == request.portion_size,
            )
            .select(["main_recipe_id", "variation_id"])
            .join(
                pl.DataFrame(
                    data={
                        "main_recipe_id": [rec.main_recipe_id for rec in selected_recipes],
                        "compliancy": [rec.compliancy for rec in selected_recipes],
                    },
                    schema_overrides={"main_recipe_id": pl.Int32},
                ),
                on="main_recipe_id",
                validate="1:1",
                coalesce=True,
            )
        )

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
            recipe_data=[PreselectorRecipeResponse(**row) for row in output_df.rows(named=True)],
            target_cost_of_food_per_recipe=cof_target_value,
            ordered_weeks_ago=generated_recipe_ids,
            error_vector=output.error_vector,  # type: ignore
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
                request.ordered_weeks_ago,
            )

        results.append(result)

    return PreselectorResult(
        success=results,
        failures=failed_weeks,
        request=request,
        model_version=model_version(),
        generated_at=datetime.now(tz=timezone.utc),
    )


async def run_preselector(
    customer: GenerateMealkitRequest,
    available_recipes: Annotated[pl.DataFrame, Menu],
    recommendations: Annotated[pl.DataFrame, RecommendatedDish],
    target_vector: pl.DataFrame,
    importance_vector: pl.DataFrame,
    store: ContractStore,
    selected_recipes: dict[int, int],
    should_explain: bool = False,
) -> PreselectorWeekOutput:
    """
    Generates a combination of recipes that best fit a personalised target and importance vector.

    Arguments:
        customer (GenerateMealkitRequest): The request defining constraints about the mealkit
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
        return PreselectorWeekOutput([], {})

    # Singlekassen
    if customer.concept_preference_ids == ["37CE056F-4779-4593-949A-42478734F747"]:
        return PreselectorWeekOutput(
            recipes=[
                PreselectorRecipe(main_recipe_id, compliance)
                for main_recipe_id in recipes["main_recipe_id"].sample(customer.number_of_recipes).to_list()
            ],
            error_vector={},
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
                company_id=pl.lit(customer.company_id),
            ),
            store=store,
        )

    if should_explain:
        import streamlit as st

        st.write("Original Menu")
        st.write(recipes)

        st.write("Raw Recipe Features")
        st.write(normalized_recipe_features)

    filtered = filter_out_unwanted_preselected_recipes(normalized_recipe_features)
    filtered, compliance, preselected_recipes = await filter_on_preferences(customer, filtered, store)
    logger.debug(f"Loading preselector recipe features: {recipes.height}")

    if filtered.height >= customer.number_of_recipes:
        normalized_recipe_features = filtered

    with duration("load-main-ingredient-catagory"):
        normalized_recipe_features = (
            await store.feature_view(RecipeMainIngredientCategory)
            .features_for(normalized_recipe_features)
            .filter(
                # Only removing those without carbo.
                # Those without protein id can be vegetarian
                pl.col("main_carbohydrate_category_id").is_not_null()
            )
            .with_subfeatures()
            .to_polars()
        )

    recipe_features = normalized_recipe_features

    with duration("load-recipe-vote"):
        recipe_features = (
            await store.feature_view(RecipeVote)
            # .select({"is_favorite", "is_dislike"})
            .features_for(recipe_features.with_columns(pl.lit(customer.agreement_id).alias("agreement_id")))
            .to_polars()
        )

    if recipe_features.height < customer.number_of_recipes:
        recipes_of_interest = set(recipes["recipe_id"].to_list()) - set(recipe_features["recipe_id"].to_list())
        logger.warning(
            f"Number of recipes are less then expected {recipe_features.height}. "
            f"Most likely due to missing features in recipes: ({recipes_of_interest}) "
            f"In portion size {customer.portion_size} - {year}, {week}"
        )
        return PreselectorWeekOutput(
            recipes=[
                PreselectorRecipe(main_recipe_id, compliance)
                for main_recipe_id in recipes.sample(customer.number_of_recipes)["main_recipe_id"].to_list()
            ],
            error_vector={},
        )

    if should_explain:
        import streamlit as st

        st.write("Recipe Candidates")
        st.write(recipe_features)

        if preselected_recipes is not None:
            st.write("Preselected recipes")
            st.write(preselected_recipes)

    if recipe_features.is_empty():
        raise ValueError(
            "Found no recipes to select from. "
            "Please let the data team know about this so we can fix it. "
            f"Had initially {recipes.height} recipes, and {recipe_features.height} recipes with features"
        )

    if not recommendations.is_empty():
        with duration("add-recommendations-data"):
            with_rank = (
                recipes.cast({"recipe_id": pl.Int32})
                .join(recommendations.select(["product_id", "order_rank"]), on="product_id", how="left")
                .unique("recipe_id")
            )

            recipe_features = (
                recipe_features.select(pl.exclude("order_rank"))
                .join(with_rank.select(["recipe_id", "order_rank"]), on="recipe_id", how="left")
                .with_columns(pl.col("order_rank").fill_null(pl.lit(recipe_features.height / 2)))
                .with_columns(
                    order_rank=pl.col("order_rank").log() / pl.col("order_rank").log().max().clip(lower_bound=1)
                )
            )
            logger.debug(
                f"Filtering based on recommendations done: {recipe_features.height}",
            )

    logger.debug(
        f"Selecting the best combination based on {recipe_features.height} recipes.",
    )

    if recipe_features.height <= customer.number_of_recipes:
        logger.warning(
            f"Too few recipes to run the preselector for agreement: {customer.agreement_id}"
            f" found only {recipe_features.height} recipes"
        )
        return PreselectorWeekOutput(
            recipes=[
                PreselectorRecipe(main_recipe_id, compliance)
                for main_recipe_id in recipes.filter(pl.col("recipe_id").is_in(recipe_features["recipe_id"]))[
                    "main_recipe_id"
                ].to_list()
            ],
            error_vector={},
        )

    with duration("compute-ordered-since"):
        recipe_features = await add_ordered_since_feature(
            customer,
            store,
            recipe_features,
            year_week=year * 100 + week,  # type: ignore
            selected_recipes=selected_recipes,
        )

    with duration("load-recipe-embeddings"):
        recipe_embeddings = (
            await store.model(RecipeEmbedding)
            .predictions_for(recipe_features.select(["main_recipe_id", "company_id"]))
            .select(["embedding"])
            .to_polars()
        )

        recipe_embeddings = recipe_embeddings.join(
            recipe_features.cast({"main_recipe_id": pl.Int32}).select(["recipe_id", "main_recipe_id"]),
            on="main_recipe_id",
        )

    if recipe_embeddings.height != recipe_features.height:
        missing_recipes = set(recipe_features["main_recipe_id"].to_list()) - set(
            recipe_embeddings["main_recipe_id"].to_list()
        )
        logger.warning(f"We are missing some embeddings for main recipe ids: {missing_recipes}")
        logger.error(f"We are missing some embeddings for main recipe ids: {missing_recipes}")

    with duration("load-attribute-scoring"):
        recipe_features = (
            await store.feature_view(AttributeScoring).features_for(recipe_features).with_subfeatures().to_polars()
        )

    assert (
        not recipe_features.is_empty()
    ), f"Found no features something is very wrong for {customer.agreement_id}, {year}, {week}"

    with duration("find-best-combination"):
        best_recipe_ids, error = await find_best_combination(
            target_vector,
            importance_vector,
            recipe_features.unique(["main_recipe_id"]),
            recipe_embeddings=recipe_embeddings,
            number_of_recipes=customer.number_of_recipes,
            preselected_recipe_ids=(
                preselected_recipes["main_recipe_id"].to_list() if preselected_recipes is not None else None
            ),
            should_explain=should_explain,
        )

    if should_explain and error is not None:
        import streamlit as st

        st.write("Error Vector:")
        st.write(error)

    if preselected_recipes is not None:
        recipes = [
            PreselectorRecipe(main_recipe_id=row["main_recipe_id"], compliancy=row["compliancy"])
            for row in preselected_recipes.filter(pl.col("main_recipe_id").is_in(best_recipe_ids)).rows(named=True)
        ]

        additional_recipes = list(set(best_recipe_ids) - set(preselected_recipes["main_recipe_id"].to_list()))
        recipes.extend(
            [PreselectorRecipe(main_recipe_id=recipe_id, compliancy=compliance) for recipe_id in additional_recipes]
        )
    else:
        recipes = [PreselectorRecipe(main_recipe_id=recipe_id, compliancy=compliance) for recipe_id in best_recipe_ids]

    return PreselectorWeekOutput(recipes=recipes, error_vector=error)
