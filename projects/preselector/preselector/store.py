import logging
from typing import Annotated

import polars as pl
from aligned import ContractStore
from data_contracts.mealkits import OneSubMealkits
from data_contracts.preselector.menu import CostOfFoodPerMenuWeek, PreselectorYearWeekMenu
from data_contracts.preselector.store import (
    FailedPreselectorOutput,
    ForecastedMealkits,
    RecipePreferences,
    SuccessfulPreselectorOutput,
)
from data_contracts.preselector.store import Preselector as PreselectorOutput
from data_contracts.recipe import NormalizedRecipeFeatures
from data_contracts.recipe_vote import RecipeVote
from data_contracts.recommendations.recommendations import RecommendatedDish
from data_contracts.recommendations.store import recommendation_feature_contracts

from preselector.monitor import duration
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek

logger = logging.getLogger(__name__)


def preselector_store() -> ContractStore:
    """
    The data-contracts needed to run the pre-selector
    """

    store = recommendation_feature_contracts()

    store.add(SuccessfulPreselectorOutput)
    store.add(FailedPreselectorOutput)
    store.add(RecipePreferences)
    store.add(PreselectorOutput)
    store.add(CostOfFoodPerMenuWeek)
    store.add(ForecastedMealkits)
    store.add(RecipeVote)
    return store


async def compute_normalized_features(recipes: pl.DataFrame, store: ContractStore) -> pl.DataFrame:
    # Fixes a bug in the data
    recipes = recipes.unique("recipe_id")

    return (
        await store.feature_view(NormalizedRecipeFeatures).features_for(recipes).drop_invalid().to_polars()
    ).with_columns(order_rank=pl.lit(0))


async def load_menu_for(company_id: str, year: int, week: int, store: ContractStore) -> pl.DataFrame:
    menus = await store.feature_view(PreselectorYearWeekMenu).all().to_lazy_polars()
    return menus.filter(
        pl.col("menu_year") == year,
        pl.col("menu_week") == week,
        pl.col("company_id").str.to_lowercase() == company_id.lower(),
    ).collect()


async def load_recommendations(
    agreement_id: int,
    year: int,
    week: int,
    store: ContractStore,
) -> pl.DataFrame:
    preds = (
        await store.model(RecommendatedDish)
        .all_predictions()
        .filter((pl.col("agreement_id") == agreement_id) & (pl.col("year") == year) & (pl.col("week") == week))
        .to_lazy_polars()
    )
    return (
        preds.sort("predicted_at", descending=True).unique(["agreement_id", "week", "year", "product_id"], keep="first")
    ).collect()


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
        f"Missing min cof '{min_cof}' for {request.company_id}, year: {year_week.year}, week: {year_week.week}, "
        f"{request.portion_size} portions. Therefore, can not normalize the cof target"
    )
    assert max_cof, f"Missing max cof '{max_cof}'. Therefore, can not normalize the cof target"
    assert (
        min_cof != max_cof
    ), f"Min and Max CoF are the same. This most likely means something is wrong for {year_week}"
    cost_of_food_value = (target_cost_of_food - min_cof) / (max_cof - min_cof)

    logger.debug(f"Normalized value from {target_cost_of_food} to {cost_of_food_value}")

    return vector.with_columns(mean_cost_of_food=pl.lit(cost_of_food_value))


async def cost_of_food_target_for(
    request: GenerateMealkitRequest, store: ContractStore
) -> Annotated[pl.DataFrame, CostOfFoodPerMenuWeek]:
    with duration("load-mealkit-cof-target"):
        cof_entities = {
            "company_id": [request.company_id] * len(request.compute_for),
            "number_of_recipes": [request.number_of_recipes] * len(request.compute_for),
            "number_of_portions": [request.portion_size] * len(request.compute_for),
            "year": [over.year for over in request.compute_for],
            "week": [over.week for over in request.compute_for],
        }

        cost_of_food = await (
            store.feature_view(CostOfFoodPerMenuWeek)
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
            default_cof, on=["company_id", "number_of_recipes", "number_of_portions"], suffix="_right"
        )
        assert defaults.height == missing.height, "Filling inn default values did not go as expected"

        cost_of_food = cost_of_food.vstack(
            defaults.with_columns(
                pl.col("cost_of_food_target_per_recipe").fill_null(pl.col("cost_of_food_target_per_recipe_right"))
            ).select(cost_of_food.columns)
        ).filter(pl.col("cost_of_food_target_per_recipe").is_not_null())

    return cost_of_food
