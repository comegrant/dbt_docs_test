import logging
from collections.abc import Callable
from datetime import datetime

from aligned import ContractStore, FeatureLocation

file_logger = logging.getLogger(__name__)


def dependency_level(
    location: FeatureLocation, store: ContractStore
) -> dict[FeatureLocation, int]:
    deps = {}
    if location.location == "feature_view":
        view = store.feature_view(location.name).view
        source = view.source

        for dep in source.depends_on():
            sub_deps = dependency_level(dep, store)

            for sub_dep, level in sub_deps.items():
                deps[sub_dep] = max(deps.get(sub_dep, 0), level)

        for dep, level in deps.items():
            deps[dep] = level + 1

    deps[location] = 0
    return deps


async def materialize_data(
    store: ContractStore,
    locations: list[FeatureLocation],
    do_freshness_check: bool = True,
    logger: Callable[[object], None] | None = None,
) -> list[tuple[FeatureLocation, int]]:
    if logger is None:
        logger = file_logger.info

    deps: dict[FeatureLocation, int] = {}
    for location in locations:
        for dep, level in dependency_level(location, store).items():
            deps[dep] = max(deps.get(dep, 0), level)

    sorted_deps = sorted(deps.items(), key=lambda x: x[1], reverse=True)

    logger(
        "Materialization order: \n\n- "
        + "\n- ".join([loc.name for loc, _ in sorted_deps])
    )

    for location, _ in sorted_deps:
        if location.location != "feature_view":
            logger(f"Skipping: {location.identifier}")
            continue

        logger(location.name)

        view = store.feature_view(location.name).view
        view_tags = view.tags or set()

        if not view.materialized_source:
            logger(f"Skipping: {location.identifier}")
            continue

        if view.event_timestamp and view.acceptable_freshness and do_freshness_check:
            try:
                freshness = await store.feature_view(location.name).freshness()
            except:  # noqa: E722
                freshness = None
            logger(freshness)

            if freshness:
                now = datetime.now(tz=freshness.tzinfo)
                if now - freshness < view.acceptable_freshness:
                    logger(f"Skipping: {location.identifier}")
                    continue
                elif "incremental" in view_tags:
                    await store.upsert_into(
                        location,
                        store.feature_view(location.name)
                        .using_source(view.source)
                        .between_dates(start_date=freshness, end_date=now),
                    )
                    continue

        await store.overwrite(
            location, store.feature_view(location.name).using_source(view.source).all()
        )

    return sorted_deps
