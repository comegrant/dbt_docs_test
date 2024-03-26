import logging
from datetime import datetime, timedelta, timezone

from aligned import FeatureStore
from aligned.feature_source import WritableFeatureSource
from aligned.local.job import DataFileReference
from aligned.schemas.feature import FeatureLocation

from rec_engine.logger import Logger

file_logger = logging.getLogger(__name__)


async def incremental_update_from_source(
    views: list[str],
    store: FeatureStore,
    logger: Logger | None = None,
) -> None:
    logger = logger or file_logger
    for view_name, view in store.feature_views.items():
        if view_name not in views:
            continue

        if not view.materialized_source:
            logger.info(f"No materialized source for {view.name} - will skip")
            continue

        if not (
            isinstance(
                view.materialized_source,
                WritableFeatureSource | DataFileReference,
            )
        ):
            logger.info(f"View: {view.name} do not have a data source that is writable")
            continue

        logger.info(
            f"Updating {view.name} batch source using source {view.source}."
            f"\nMaterializing to {view.materialized_source}",
        )

        location = FeatureLocation.feature_view(view_name)
        freshness = await store.feature_view(view_name).freshness()
        source_store = store.feature_view(view_name).using_source(view.source)

        if freshness:
            new_feature_job = source_store.between_dates(
                start_date=freshness,
                end_date=datetime.now(tz=timezone.utc),
            )
            await store.upsert_into(location, new_feature_job)
        else:
            await source_store.all().write_to_source(view.materialized_source)


async def update_from_source(
    views: list[str],
    store: FeatureStore,
    logger: Logger | None = None,
) -> None:
    logger = logger or file_logger
    for view_name, view in store.feature_views.items():
        if view_name not in views:
            continue

        if not view.source:
            logger.info(f"No staging source for {view.name} - will skip")
            continue

        logger.info(
            f"Updating {view.name} batch source using staging source {view.source}",
        )
        if not (
            isinstance(
                view.materialized_source,
                WritableFeatureSource | DataFileReference,
            )
        ):
            logger.info(f"View: {view.name} do not have a data source that is writable")
            continue

        await store.feature_view(view_name).using_source(
            view.source,
        ).all().write_to_source(view.materialized_source)


async def update_models_from_source_if_older_than(
    threshold: timedelta,
    models: list[str],
    store: FeatureStore,
    logger: Logger | None = None,
) -> None:
    now = datetime.now(tz=timezone.utc)
    views_to_update: set[str] = set()
    logger = logger or file_logger
    for model in models:
        all_freshnesses = await store.model(model).freshness()
        for location, freshness in all_freshnesses.items():
            if location.location != "feature_view":
                continue

            if not freshness:
                views_to_update.add(location.name)
                continue

            if freshness.tzinfo is None:
                freshness_tz = freshness.replace(tzinfo=timezone.utc)
            else:
                freshness_tz = freshness

            logger.info(
                f"The freshness timestamp for {location.name} is {freshness_tz}.",
            )
            difference = now - freshness_tz
            logger.info(f"Freshenss for {location.name} is {difference}.")

            if difference.total_seconds() > threshold.total_seconds():
                views_to_update.add(location.name)
                logger.info(f"{location.name} is older than {threshold}")
            else:
                logger.info(f"{location.name} is fresh enough")

    await update_from_source(list(views_to_update), store, logger=logger)


async def update_view_from_source_if_older_than(
    threshold: timedelta,
    views: list[str],
    store: FeatureStore,
    logger: Logger | None = None,
) -> None:
    now = datetime.now(tz=timezone.utc)
    views_to_update = set()
    logger = logger or file_logger

    for view in views:
        freshness = await store.feature_view(view).freshness()

        if not freshness:
            views_to_update.add(view)
            continue

        if freshness.tzinfo is None:
            freshness = freshness.replace(tzinfo=timezone.utc)

        difference = now - freshness
        logger.info(f"The freshness timestamp for {view} is {freshness}.")
        logger.info(f"Freshenss for {view} is {difference}.")

        if difference.total_seconds() > threshold.total_seconds():
            views_to_update.add(view)

    await update_from_source(list(views_to_update), store, logger=logger)
