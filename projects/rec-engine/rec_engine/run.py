import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import dotenv
import pandas as pd
from aligned import FeatureStore
from aligned.feature_store import ConvertableToRetrivalJob, RetrivalJob
from data_contracts.sql_server import SqlServerConfig

from rec_engine.clustering import ClusterModel
from rec_engine.content_based import CBModel
from rec_engine.log_step import log_step
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

logger = logging.getLogger(__name__)


@dataclass
class RateMenuRecipes:
    year_weeks: list[int]
    recipe_ids: list[int]
    product_id: list[str]


@dataclass
class ManualDataset:
    train_on_recipe_ids: list[int]
    rate_menus: RateMenuRecipes


@dataclass
class CompanyDataset:
    company_id: str
    year_weeks_to_predict_on: list[int]
    year_weeks_to_train_on: list[int]
    db_to_use: SqlServerConfig = field(default_factory=lambda: adb)


def backup_recommendations(recommendations: pd.DataFrame) -> pd.DataFrame:
    recs = (
        recommendations[["recipe_id", "score"]]
        .groupby("recipe_id", as_index=False)
        .median()
    )
    recs["predicted_at"] = datetime.utcnow()
    return recs


async def run(
    dataset: ManualDataset | CompanyDataset,
    store: FeatureStore,
    agreement_ids_subset: list[int] | None = None,
    run_id: str | None = None,
    model_regristry: ModelRegistry = InMemoryModelRegistry(),
    write_to_path: str | None = "data/rec_engine",
    number_of_recommendations_per_week: int = 8,
    update_source_threshold: timedelta | None = None,
    ratings_update_source_threshold: timedelta | None = None,
    ratings_view: str = "historical_recipe_orders",
    recipe_rating_contract: str = "user_recipe_likability",
    recipe_cluster_contract: str = "recipe_cluster",
) -> None:
    if not run_id:
        run_id = str(datetime.utcnow().isoformat())

    if not dotenv.load_dotenv():
        logger.info(
            "Unable to load .env file. This can break things as env vars are needed",
        )

    if update_source_threshold:
        with log_step("Updating model freshness"):
            model_contracts = [recipe_rating_contract, recipe_cluster_contract]

            await update_models_from_source_if_older_than(
                threshold=update_source_threshold, models=model_contracts, store=store,
            )

    if ratings_update_source_threshold:
        with log_step("Updating ratings view"):
            views = [ratings_view]
            await update_view_from_source_if_older_than(
                threshold=ratings_update_source_threshold, views=views, store=store,
            )

    with log_step("Selecting who to train and predict for"):
        recipe_entities, menus = await select_entities(dataset)

    company_id = "Testing Run"
    if isinstance(dataset, CompanyDataset):
        company_id = dataset.company_id

    with log_step("Training CB Model"):
        model_id = f"{run_id}_{recipe_rating_contract}"
        rating_model = await CBModel.train_using(
            recipe_entities,
            store,
            model_contract_name=recipe_rating_contract,
            ratings_view=ratings_view,
            agreement_ids_subset=agreement_ids_subset,
            model_version=model_id,
        )
        model_regristry.store_model(rating_model, model_id)

    with log_step("Training cluster model"):
        for yearweek in menus["yearweek"].unique():
            weekly_recipes = menus[menus["yearweek"] == yearweek]

            model_id = f"{run_id}_{recipe_cluster_contract}_{yearweek}"
            model = await ClusterModel.train_using(
                weekly_recipes,
                store,
                model_contract=recipe_cluster_contract,
                model_version=model_id,
            )
            model_regristry.store_model(model, model_id)

    # Makes sure we load the features from the cache rather then ADB / Data Lake
    #     feature_cache_location,

    if write_to_path:
        with log_step(f"Setting sources to local file system {write_to_path}"):
            contracts = list(store.models.keys())
            store = use_local_sources_in(store, contracts, write_to_path)

    with log_step("Predict user-recipe likability"):
        ratings_preds = await rating_model.predict_over(menus, store)

    with log_step(f"Store {ratings_preds.shape[0]} user-recipe likability predictions"):
        logger.info(ratings_preds.head())
        await store.model(rating_model.model_contract_name).insert_predictions(
            ratings_preds,
        )

    with log_step("Predict backup recommendations"):
        backup_recs = backup_recommendations(ratings_preds)

    with log_step(f"Storing {backup_recs.shape[0]} backup recommendations"):
        logger.info(backup_recs.head())
        await store.model("backup_recommendations").insert_predictions(backup_recs)

    for yearweek in menus["yearweek"].unique():
        model_id = f"{run_id}_{recipe_cluster_contract}_{yearweek}"
        cluster_model = model_regristry.load_model(model_id)

        if not cluster_model:
            raise ValueError(
                f"Unable to find model with id {model_id} in model registry",
            )

        with log_step("Predict recipe cluster"):
            cluster_preds = await cluster_model.predict_over(
                menus[menus["yearweek"] == yearweek], store,
            )

        with log_step(f"Storing {cluster_preds.shape[0]} cluster predictions"):
            logger.info(cluster_preds.head())
            await store.model(cluster_model.model_contract_name).insert_predictions(
                cluster_preds,
            )

    with log_step("Predicting ranking for recipes"):
        # Should in theory be the menu recipe ids x agreement ids
        agreement_ids = ratings_preds["agreement_id"].unique()
        menu_per_agreement = menus.loc[menus.index.repeat(agreement_ids.shape[0])]

        # Need to do to list, as this will repeat the list n times, and not the items n items
        menu_per_agreement["agreement_id"] = agreement_ids.tolist() * menus.shape[0]

        recipes_to_rank_entities = menu_per_agreement.reset_index(drop=True)
        rec_store = store.model("rec_engine")
        recipes_to_rank = await rec_store.features_for(
            recipes_to_rank_entities,
        ).to_pandas()
        ranking = predict_rankings(recipes_to_rank, menus)
        ranking["company_id"] = company_id

    with log_step(f"Writing {ranking.shape[0]} rankings for point in time storage"):
        logger.info(ranking.head())
        await rec_store.insert_predictions(ranking)

    if write_to_path is None and rec_store.model.predictions_view.application_source:
        with log_step(f"Writing {ranking.shape[0]} rankings to application source"):
            logger.info(ranking.head())
            await rec_store.using_source(
                rec_store.model.predictions_view.application_source,
            ).upsert_predictions(ranking)

    with log_step("Formatting frontend format"):
        formatted_recommendations = format_ranking_recommendations(
            ranking, number_of_recommendations_per_week,
        )
        formatted_recommendations["run_timestamp"] = datetime.utcnow()
        formatted_recommendations["company_id"] = company_id

    with log_step(f"Writing {formatted_recommendations.shape[0]} to frontend source"):
        logger.info(formatted_recommendations.head())
        await store.model("presented_recommendations").upsert_predictions(
            formatted_recommendations,
        )


def format_ranking_recommendations(
    rankings: pd.DataFrame, number_of_recommendations: int,
) -> pd.DataFrame:
    column = "order_of_relevance_cluster"
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Selects the entities to train and predict over.
    This can either be a manually set of recipe_ids, and the menus to predict over.
    Or it can be dynamically set based on a company constraint.
    """
    recommend_for_entities: RetrivalJob | ConvertableToRetrivalJob
    if isinstance(dataset, CompanyDataset):
        year_week_strings = ", ".join(
            [str(yw) for yw in dataset.year_weeks_to_predict_on],
        )
        year_week_train_string = ", ".join(
            [str(yw) for yw in dataset.year_weeks_to_train_on],
        )

        recipes = dataset.db_to_use.fetch(
            f"""SELECT DISTINCT (menu_year * 100 + menu_week) as yearweek, wm.menu_year as year, wm.menu_week as week, mr.recipe_id, m.MENU_EXTERNAL_ID as product_id
FROM pim.MENU_RECIPES mr
INNER JOIN pim.MENUS m ON m.MENU_ID = mr.MENU_ID AND m.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
INNER JOIN pim.weekly_menus wm ON m.WEEKLY_MENUS_ID = wm.weekly_menus_id
INNER JOIN pim.MENU_VARIATIONS mv ON m.MENU_ID = mv.MENU_ID
WHERE company_id = '{dataset.company_id}'
    AND menu_year * 100 + menu_week IN ({year_week_strings})
    AND mv.PORTION_ID != 7 -- Removing protion size of 1""",
        )

        train_on_entities = await dataset.db_to_use.fetch(
            f"""SELECT DISTINCT mr.recipe_id
FROM pim.MENU_RECIPES mr
INNER JOIN pim.MENUS m ON m.MENU_ID = mr.MENU_ID AND m.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
INNER JOIN pim.weekly_menus wm ON m.WEEKLY_MENUS_ID = wm.weekly_menus_id
INNER JOIN pim.MENU_VARIATIONS mv ON m.MENU_ID = mv.MENU_ID
WHERE company_id = '{dataset.company_id}'
    AND menu_year * 100 + menu_week IN ({year_week_train_string})
    AND mv.PORTION_ID != 7 -- Removing protion size of 1""",
        ).to_pandas()

        menu_job = await recipes.to_pandas()

        # Want to recommend for all recipes in a company
        # However, it looks like this is not able to fetch unless it is attached to a week menu
        # Therefore, just using the selected recipes in our year weeks of interest
        recommend_for_entities = train_on_entities.drop_duplicates()

    elif isinstance(dataset, ManualDataset):
        recommend_for_entities = {"recipe_id": dataset.train_on_recipe_ids}

        menus = dataset.rate_menus
        if len(menus.recipe_ids) != len(menus.year_weeks):
            raise ValueError(
                "The menu recipe ids and year weeks arrays need to be of equal size. As the recommendation will be assosiated with a yearweek",
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
        logger.info(f"Using custom menus: {menu_job}")
    else:
        raise ValueError(
            f"Unsupported dataset {type(dataset)} expected either a CompanyDataset, or ManualDataset",
        )

    if isinstance(recommend_for_entities, RetrivalJob):
        recipe_df = await recommend_for_entities.to_pandas()
    else:
        recipe_df = pd.DataFrame(recommend_for_entities)

    return recipe_df, menu_job
