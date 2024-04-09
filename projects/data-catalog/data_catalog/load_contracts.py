import logging

from aligned import FeatureStore

logger = logging.getLogger(__name__)


def inject_sources() -> None:
    from aligned.data_source.batch_data_source import BatchDataSourceFactory
    from aligned.sources.azure_blob_storage import (
        AzureBlobCsvDataSource,
        AzureBlobDeltaDataSource,
        AzureBlobParquetDataSource,
    )
    from data_contracts.sql_server import SqlServerDataSource

    for data_source in [
        AzureBlobParquetDataSource,
        AzureBlobCsvDataSource,
        AzureBlobDeltaDataSource,
        SqlServerDataSource,
    ]:
        if data_source.type_name in BatchDataSourceFactory.shared().supported_data_sources:
            continue

        BatchDataSourceFactory.shared().supported_data_sources[data_source.type_name] = data_source


async def custom_store() -> FeatureStore:
    from data_contracts.preselector.store import preselector_contracts
    from data_contracts.recommendations.store import recommendation_feature_contracts

    inject_sources()

    return preselector_contracts().combine(recommendation_feature_contracts())
