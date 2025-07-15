import logging
from datetime import date

import polars as pl
from aligned import ContractStore
from aligned.data_source.batch_data_source import BatchDataSource
from aligned.feature_store import ConvertableToLocation
from aligned.sources.in_mem_source import InMemorySource
from data_contracts.preselector.menu import PreselectorYearWeekMenu
from data_contracts.preselector.store import Preselector as PreselectorOutput
from data_contracts.recipe import NormalizedRecipeFeatures, RecipeCost

from preselector.process_stream import load_cache_for
from preselector.store import preselector_store

logger = logging.getLogger(__name__)


store: ContractStore | None = None

CacheSourceConfig = tuple[ConvertableToLocation, BatchDataSource, pl.Expr | None]


async def load_store() -> ContractStore:
    global store  # noqa: PLW0603
    if store is not None:
        return store

    store = preselector_store()
    store, cache_sources = remove_user_sources(store)

    logger.info(f"Loading data for {len(cache_sources)}")
    store = await load_cache_for(store, cache_sources)
    logger.info("Cache is hot")
    return store


def remove_user_sources(store: ContractStore) -> tuple[ContractStore, list[CacheSourceConfig]]:
    today = date.today()

    depends_on = store.feature_view(PreselectorOutput).view.source.depends_on()

    cache_sources: list[CacheSourceConfig] = [
        (
            RecipeCost.location,
            InMemorySource.empty(),
            (pl.col("menu_year") >= today.year),
        ),
        (
            PreselectorYearWeekMenu.location,
            InMemorySource.empty(),
            (pl.col("menu_year") >= today.year),
        ),
        (
            NormalizedRecipeFeatures.location,
            InMemorySource.empty(),
            (pl.col("year") >= today.year),
        ),
    ]
    custom_cache = {loc for loc, _, _ in cache_sources}

    for dep in depends_on:
        if dep in custom_cache:
            continue

        if dep.location_type == "feature_view":
            view = store.feature_view(dep.name).view
        else:
            view = store.model(dep.name).model.predictions_view.as_view(dep.name)

        entity_names = view.entitiy_names
        in_mem_source = InMemorySource.empty().with_view(view)

        if "agreement_id" in entity_names or "billing_agreement_id" in entity_names:
            # Just an empty frame with all columns that are expected
            logger.info(f"Removing real source for '{dep.name}'")
            store = store.update_source_for(dep, in_mem_source)
        else:
            cache_sources.append((dep, in_mem_source, None))

    return (store, cache_sources)
