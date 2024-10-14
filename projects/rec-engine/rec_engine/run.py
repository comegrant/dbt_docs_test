import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Annotated

import dotenv
import pandas as pd
import polars as pl
from aligned import FeatureLocation, FeatureStore
from aligned.feature_store import ConvertableToRetrivalJob, RetrivalJob
from data_contracts.menu import YearWeekMenu
from data_contracts.orders import MealboxChangesAsRating
from data_contracts.sql_server import SqlServerConfig

from rec_engine.content_based import CBModel
from rec_engine.log_step import log_step
from rec_engine.logger import Logger
from rec_engine.model_registry import (
    InMemoryModelRegistry,
    ModelRegistry,
)
from rec_engine.ranking_model import predict_rankings
from rec_engine.source_selector import use_local_sources_in
from rec_engine.sources import adb
from rec_engine.update_source import (
    update_models_from_source_if_older_than,
    update_view_from_source_if_older_than,
)

file_logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class RateMenuRecipes:
    year_weeks: list[int]
    recipe_ids: list[int]
    product_id: list[str]


@dataclass
class ManualDataset:
    train_on_recipe_ids: list[int]
    train_on_agreement_ids: list[int]
    rate_menus: RateMenuRecipes


@dataclass
class CompanyDataset:
    company_id: str
    year_weeks_to_predict_on: list[int]
    year_weeks_to_train_on: list[int]
    only_for_agreement_ids: list[int] | None = None
    db_to_use: SqlServerConfig = field(default_factory=lambda: adb)


def backup_recommendations(recommendations: pd.DataFrame) -> pd.DataFrame:
    recs = recommendations[["recipe_id", "score"]].groupby("recipe_id", as_index=False).median()
    recs["predicted_at"] = datetime.now(tz=timezone.utc)
    return recs


async def run(
    dataset: ManualDataset | CompanyDataset,
    store: FeatureStore,
    run_id: str | None = None,
    model_regristry: ModelRegistry | None = None,
    write_to_path: str | None = "data/rec_engine",
    update_source_threshold: timedelta | None = None,
    ratings_update_source_threshold: timedelta | None = None,
    ratings_view: str = "historical_recipe_orders",
    behavior_ratings_view: str = "mealbox_changes_as_rating",
    recipe_rating_contract: str = "user_recipe_likability",
    recipe_cluster_contract: str = "recipe_cluster",
    logger: Logger | None = None,
) -> None:
    logger = logger or file_logger

    if model_regristry is None:
        model_regristry = InMemoryModelRegistry()

    if not run_id:
        run_id = str(datetime.now(tz=timezone.utc).isoformat())

    if not dotenv.load_dotenv():
        logger.info(
            "Unable to load .env file. This can break things if you have not set secrets in another way",
        )

    if update_source_threshold:
        with log_step("Updating model freshness", logger=logger):
            model_contracts = [recipe_rating_contract, recipe_cluster_contract]

            await update_models_from_source_if_older_than(
                threshold=update_source_threshold,
                models=model_contracts,
                store=store,
                logger=logger,
            )

    if ratings_update_source_threshold:
        with log_step("Updating ratings view", logger=logger):
            views = [ratings_view, behavior_ratings_view]
            await update_view_from_source_if_older_than(
                threshold=ratings_update_source_threshold,
                views=views,
                store=store,
                logger=logger,
            )

    with log_step("Selecting who to train and predict for", logger=logger):
        recipe_entities, menus = await select_entities(dataset, logger=logger)

    company_id = "Testing Run"
    if isinstance(dataset, CompanyDataset):
        company_id = dataset.company_id

    with log_step("Training CB Model", logger=logger):
        model_id = f"{run_id}_{recipe_rating_contract}"
        rating_model = await CBModel.train_using(
            recipe_entities,
            store,
            model_contract_name=recipe_rating_contract,
            explicit_rating_view=ratings_view,
            behavioral_rating_view=behavior_ratings_view,
            model_version=model_id,
            logger=logger,
        )
        model_regristry.store_model(rating_model, model_id)

    # Makes sure we load the features from the cache rather then ADB / Data Lake
    #     feature_cache_location,
    if write_to_path:
        with log_step(
            f"Setting sources to local file system {write_to_path}",
            logger=logger,
        ):
            contracts = list(store.models.keys())
            store = use_local_sources_in(store, contracts, write_to_path, logger=logger)

    with log_step("Predict user-recipe likability", logger=logger):
        ratings_preds = await rating_model.predict_over(menus, store, logger=logger)
        ratings_preds["company_id"] = company_id

    with log_step(
        f"Store {ratings_preds.shape[0]} user-recipe likability predictions",
        logger=logger,
    ):
        logger.info(ratings_preds.head())
        await store.upsert_into(
            FeatureLocation.model(rating_model.model_contract_name),
            ratings_preds,
        )

    with log_step("Predicting ranking for recipes", logger=logger):
        # Should in theory be the menu recipe ids x agreement ids
        agreement_ids = ratings_preds["agreement_id"].unique()

        menu_per_agreement = menus.loc[menus.index.repeat(agreement_ids.shape[0])]

        # Need to do to list, as this will repeat the list n times, and not the items n items
        menu_per_agreement["agreement_id"] = agreement_ids.tolist() * menus.shape[0]

        recipes_to_rank_entities = menu_per_agreement.reset_index(drop=True)
        recipes_to_rank_entities["company_id"] = company_id

        rec_store = store.model("rec_engine")
        recipes_to_rank = (
            await rec_store.features_for(
                recipes_to_rank_entities,
            ).to_polars()
        ).to_pandas()
        ranking = predict_rankings(recipes_to_rank, menus, logger=logger)
        ranking["company_id"] = company_id

    with log_step(
        f"Writing {ranking.shape[0]} rankings for point in time storage",
        logger=logger,
    ):
        logger.info(ranking.head())
        await rec_store.upsert_predictions(ranking)


def format_ranking_recommendations(
    rankings: pd.DataFrame,
    number_of_recommendations: int,
) -> pd.DataFrame:
    column = "order_rank"
    year_week_products = (
        rankings[rankings[column] <= number_of_recommendations]
        .groupby(["agreement_id", "year", "week"])
        .apply(lambda x: x.sort_values(column)["product_id"].to_list())
        .reset_index()
        .rename(columns={0: "product_ids"})
    )

    return (
        year_week_products.assign(
            json_row=year_week_products.apply(
                lambda row: {
                    "year": row["year"],
                    "week": row["week"],
                    "productIds": row["product_ids"],
                },
                axis=1,
            ),
        )
        .groupby("agreement_id")
        .apply(lambda week_preds: json.dumps(week_preds["json_row"].to_list()))
        .reset_index()
        .rename(columns={0: "recommendation_json"})
    )


async def select_entities(
    dataset: ManualDataset | CompanyDataset,
    logger: Logger | None = None,
) -> tuple[Annotated[pd.DataFrame, "entities to train over"], Annotated[pd.DataFrame, "entities to predict over"]]:
    """
    Selects the entities to train and predict over.
    This can either be a manually set of recipe_ids, and the menus to predict over.
    Or it can be dynamically set based on a company constraint.
    """
    func_logger = logger or file_logger
    recommend_for_entities: RetrivalJob | ConvertableToRetrivalJob
    if isinstance(dataset, CompanyDataset):
        year_week_strings = ", ".join(
            [str(yw) for yw in dataset.year_weeks_to_predict_on],
        )
        func_logger.info(
            f"Using company recipes: {dataset.company_id} and year weeks: {year_week_strings}",
        )

        recipes = await YearWeekMenu.query().all().to_lazy_polars()
        recipes = (
            recipes.filter(
                pl.col("company_id") == dataset.company_id,
                pl.col("yearweek").is_in(dataset.year_weeks_to_predict_on),
            )
            .unique(["company_id", "yearweek", "product_id"])
            .collect()
        )

        ratings_filter = []
        if dataset.year_weeks_to_train_on:
            ratings_filter.append(
                (pl.col("year") * 100 + pl.col("week")).is_in(
                    dataset.year_weeks_to_train_on,
                ),
            )
        if dataset.only_for_agreement_ids:
            ratings_filter.append(
                pl.col("agreement_id").is_in(dataset.only_for_agreement_ids),
            )
        else:
            ratings_filter = [
                pl.col("company_id") == dataset.company_id,
            ]

        train_on_entities = (
            (await MealboxChangesAsRating.query().all().to_lazy_polars())
            .filter(
                *ratings_filter,
            )
            .select(["recipe_id", "agreement_id"])
            .collect()
            .to_pandas()
        )

        menu_job = recipes.to_pandas()

        # Want to recommend for all recipes in a company
        # However, it looks like this is not able to fetch unless it is attached to a week menu
        # Therefore, just using the selected recipes in our year weeks of interest
        recommend_for_entities = train_on_entities.drop_duplicates()

    elif isinstance(dataset, ManualDataset):
        recommend_for_entities = {
            "recipe_id": dataset.train_on_recipe_ids,
            "agreement_id": dataset.train_on_agreement_ids,
        }

        menus = dataset.rate_menus
        if len(menus.recipe_ids) != len(menus.year_weeks):
            raise ValueError(
                "The menu recipe ids and year weeks arrays need to be of equal size. "
                "As the recommendation will be assosiated with a yearweek",
            )

        menu_job = pd.DataFrame(
            {
                "recipe_id": menus.recipe_ids,
                "yearweek": menus.year_weeks,
                "product_id": menus.product_id,
                "year": [int(yw / 100) for yw in menus.year_weeks],
                "week": [int(yw % 100) for yw in menus.year_weeks],
            },
        )
        func_logger.info(f"Using custom menus: {menu_job}")
    else:
        raise ValueError(
            f"Unsupported dataset {type(dataset)} expected either a CompanyDataset, or ManualDataset",
        )

    if isinstance(recommend_for_entities, RetrivalJob):
        recipe_df = await recommend_for_entities.to_pandas()
    else:
        recipe_df = pd.DataFrame(recommend_for_entities)

    return recipe_df, menu_job
