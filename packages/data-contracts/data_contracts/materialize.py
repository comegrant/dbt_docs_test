import logging
from collections.abc import Callable
from datetime import datetime

from aligned import ContractStore, FeatureLocation

file_logger = logging.getLogger(__name__)


def dependency_level(
    location: FeatureLocation, store: ContractStore
) -> dict[FeatureLocation, int]:
    deps = {}

    loc_deps = set()

    if location.location_type == "feature_view":
        view = store.feature_view(location.name).view
        source = view.source
        loc_deps = source.depends_on()


    elif location.location_type == "model":
        model = store.model(location.name)
        loc_deps = model.depends_on()

    for dep in loc_deps:
        sub_deps = dependency_level(dep, store)

        for sub_dep, level in sub_deps.items():
            deps[sub_dep] = max(deps.get(sub_dep, 0), level)

    for dep, level in deps.items():
        deps[dep] = level + 1

    deps[location] = 0

    return deps

async def materialize_view(
    location: FeatureLocation,
    should_force_update: bool,
    store: ContractStore,
    logger: Callable[[object], None]
) -> None:
    logger(location.name)

    view = store.feature_view(location.name).view
    view_tags = view.tags or set()

    if not view.materialized_source:
        logger(f"Skipping: {location.identifier}")
        return

    if not should_force_update and view.event_timestamp and view.acceptable_freshness:
        try:
            freshness = await store.feature_view(location.name).freshness()
        except Exception as e:
            logger(f"Got error when loading freshness: {e}")
            freshness = None
        logger(freshness)

        if freshness:
            now = datetime.now(tz=freshness.tzinfo)
            if now - freshness < view.acceptable_freshness:
                logger(f"Skipping: {location.identifier}")
                return
            elif "incremental" in view_tags:
                await store.upsert_into(
                    location,
                    store.feature_view(location.name)
                    .using_source(view.source)
                    .between_dates(start_date=freshness, end_date=now)
                )
                return
    else:
        logger(
            "Did not check freshness since one of the following was evaluated to false"
            f"Force Upadate: {should_force_update}\n"
            f"Event Timestamp: {view.event_timestamp}\n"
            f"Freshness: {view.acceptable_freshness}"
        )

    await store.overwrite(
        location, store.feature_view(location.name).using_source(view.source).all()
    )

async def materialize_model(
    location: FeatureLocation,
    should_force_update: bool,
    store: ContractStore,
    logger: Callable[[object], None]
) -> None:
    logger(location.name)

    model = store.model(location.name).model
    pred_view = model.predictions_view
    model_tags = model.tags or set()
    model_store = store.model(location.name)

    if not pred_view.source:
        logger(f"Skipping: {location.identifier}")
        return

    if model.exposed_model is None:
        logger(f"Missing an exposed model for: {location.identifier}")
        return

    requests = model_store.input_request().needed_requests

    if len(requests) != 1:
        logger(f"Model {model.name} have more then one input source. Will not predict.")
        return

    input_request = requests[0]
    input_loc = input_request.location
    if input_loc.location_type != "feature_view":
        logger(
            f"Expected a feature view as input, but got {input_loc.location_type}. "
            f"Will therefore skip {location.name}"
        )
        return

    if not should_force_update and pred_view.event_timestamp and "incremental" in model_tags:
        try:
            freshness = await store.model(location.name).prediction_freshness()
        except Exception as e:
            logger(f"Got error when loading freshness: {e}")
            freshness = None
        logger(freshness)

        if freshness:
            now = datetime.now(tz=freshness.tzinfo)
            if pred_view.acceptable_freshness and now - freshness < pred_view.acceptable_freshness:
                logger(f"Skipping: {location.identifier}")
                return
            else:
                await store.upsert_into(
                    location,
                    store.model(location.name).predict_over(
                        store.feature_view(input_loc.name).between_dates(
                            start_date=freshness, end_date=now
                        )
                    ).with_subfeatures()
                )
                return
    else:
        logger(
            "Did not check freshness since one of the following was evaluated to false"
            f"Force Upadate: {should_force_update}\n"
            f"Event Timestamp: {pred_view.event_timestamp}\n"
            f"Freshness: {pred_view.acceptable_freshness}"
        )

    preds = store.model(location.name).predict_over(
        store.feature_view(input_loc.name).all()
    ).with_subfeatures()
    await store.overwrite(location, preds)


async def materialize_data(
    store: ContractStore,
    locations: list[FeatureLocation],
    should_force_update: bool,
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
        if location.location_type == "feature_view":
            await materialize_view(
                location, should_force_update=should_force_update, store=store, logger=logger
            )
        else:
            await materialize_model(
                location, should_force_update=should_force_update, store=store, logger=logger
            )

    return sorted_deps
