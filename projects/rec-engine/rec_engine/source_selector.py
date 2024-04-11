import logging
from pathlib import Path

from aligned import FeatureStore, FileSource
from aligned.feature_source import BatchFeatureSource
from aligned.schemas.feature import FeatureLocation

from rec_engine.logger import Logger

file_logger: Logger = logging.getLogger(__name__)


def use_cache_for_model_inputs(
    cache_file: str,
    models: list[str],
    store: FeatureStore,
) -> FeatureStore:
    if not isinstance(store.feature_source, BatchFeatureSource):
        raise ValueError(
            "Unable to set local sources when the feature source is not a BatchFeatureSource",
        )

    sources = store.feature_source

    for model in models:
        for loc in store.model(model).depends_on():
            # makes the store read the locally cached files
            sources.sources[loc.identifier] = FileSource.parquet_at(cache_file)

    return store.with_source(sources)


def use_local_sources_in(
    store: FeatureStore,
    contracts: list[str],
    write_to_path: str,
    logger: Logger | None = None,
) -> FeatureStore:
    """
    Sets the feature sources to a local dir.
    This can help improve performance, or make debugging easier as results are written to a local dir
    """
    if not isinstance(store.feature_source, BatchFeatureSource):
        raise ValueError(
            "Unable to set local sources when the feature source is not a BatchFeatureSource",
        )

    logger = logger or file_logger

    sources = store.feature_source

    # Changing the sources of our models to a local one
    # A bit to impl spesific, but will do for now
    write_path = Path(write_to_path)
    if not write_path.exists():
        write_path.mkdir(parents=True)

    for contract in contracts:
        path = f"{write_to_path}/{contract}.csv"
        logger.info(f"Writing '{contract}' to local file system {path}")
        sources.sources[FeatureLocation.model(contract).identifier] = FileSource.csv_at(
            path,
        )

    return store.with_source(sources)
