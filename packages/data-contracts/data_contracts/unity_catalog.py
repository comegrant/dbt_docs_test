from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl
from aligned import EventTimestamp
from aligned.data_source.batch_data_source import (
    BatchDataSource,
    FeatureType,
)
from aligned.feature_source import WritableFeatureSource
from aligned.retrival_job import RetrivalJob, RetrivalRequest
from aligned.sources.local import FileFactualJob

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def is_running_on_databricks() -> bool:
    import os
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

@dataclass
class DatabricksConnectionConfig:

    token: str
    host: str
    cluster_id: str

    @staticmethod
    def on_databricks_only() -> 'DatabricksConnectionConfig':
        return DatabricksConnectionConfig("", "", "")

    def catalog(self, catalog: str) -> 'UnityCatalog':
        return UnityCatalog(self, catalog)

    def connection(self) -> 'SparkSession':
        if is_running_on_databricks() or self.token == "":
            from databricks.sdk.runtime import spark

            return spark

        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.host(self.host).token(self.token).clusterId(self.cluster_id).getOrCreate()


@dataclass
class UnityCatalog:
    config: DatabricksConnectionConfig

    catalog: str

    def schema(self, schema: str) -> 'UnityCatalogSchema':
        return UnityCatalogSchema(self.config, self.catalog, schema)


@dataclass
class UnityCatalogSchema:
    config: DatabricksConnectionConfig

    catalog: str
    schema: str

    def table(self, table: str) -> 'UCTableSource':
        return UCTableSource(
            self.config,
            UnityCatalogTableConfig(self.catalog, self.schema, table)
        )

    def feature_table(self, table: str) -> 'UCFeatureTableSource':
        return UCFeatureTableSource(
            self.config,
            UnityCatalogTableConfig(self.catalog, self.schema, table)
        )


@dataclass
class UnityCatalogTableConfig:
    catalog: str
    schema: str
    table: str

    def identifier(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass
class UCFeatureTableSource(BatchDataSource, WritableFeatureSource):

    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig

    def job_group_key(self) -> str:
        return "uc_feature_table"

    def all_data(self, request: RetrivalRequest, limit: int | None) -> RetrivalJob:
        from databricks.feature_engineering import FeatureEngineeringClient

        client = FeatureEngineeringClient()

        async def load() -> pl.LazyFrame:
            spark_df = client.read_table(name=self.table.identifier())

            if limit:
                spark_df = spark_df.limit(limit)

            return pl.from_pandas(spark_df.toPandas()).lazy()

        return RetrivalJob.from_lazy_function(load, request)

    def all_between_dates(
        self,
        request: RetrivalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrivalJob:

        raise NotImplementedError(type(self))

    @classmethod
    def multi_source_features_for(
        cls, facts: RetrivalJob, requests: list[tuple['UCFeatureTableSource', RetrivalRequest]] # noqa: ANN102
    ) -> RetrivalJob:
        from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

        keys = {
            source.job_group_key() for source, _ in requests if isinstance(source, BatchDataSource)
        }
        if len(keys) != 1:
            raise NotImplementedError(
                f'Type: {cls} have not implemented how to load fact data with multiple sources.'
            )

        client = FeatureEngineeringClient()

        result_request: RetrivalRequest | None = None
        lookups = []

        for source, request in requests:
            lookups.append(
                FeatureLookup(
                    source.table.identifier(),
                    lookup_key=list(request.entity_names),
                    feature_names=request.feature_names,
                    timestamp_lookup_key=request.event_timestamp.name if request.event_timestamp else None
                )
            )

            if result_request is None:
                result_request = request
            else:
                result_request  = result_request.unsafe_combine(request)

        assert lookups, "Found no lookups"
        assert result_request, "A `request_result` was supposed to be created."

        async def load() -> pl.LazyFrame:
            import pyspark.pandas as ps

            df = await facts.to_pandas()

            dataset = client.create_training_set(
                df=ps.from_pandas(df), # type: ignore
                feature_lookups=lookups,
                label=None,
                exclude_columns=None
            )

            return pl.from_pandas(dataset.load_df().toPandas()).lazy()

        return RetrivalJob.from_lazy_function(load, result_request)


    def features_for(self, facts: RetrivalJob, request: RetrivalRequest) -> RetrivalJob:
        return type(self).multi_source_features_for(facts, [(self, request)])

    async def schema(self) -> dict[str, FeatureType]:
        """Returns the schema for the data source

        ```python
        source = FileSource.parquet_at('test_data/titanic.parquet')
        schema = await source.schema()
        >>> {'passenger_id': FeatureType(name='int64'), ...}
        ```

        Returns:
            dict[str, FeatureType]: A dictionary containing the column name and the feature type
        """
        raise NotImplementedError(f'`schema()` is not implemented for {type(self)}.')

    async def freshness(self, event_timestamp: EventTimestamp) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        spark = self.config.connection()
        return spark.sql(
            f"SELECT MAX({event_timestamp.name}) as {event_timestamp.name} FROM {self.table.identifier()}"
        ).toPandas()[event_timestamp.name].to_list()[0]


    async def insert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        raise NotImplementedError(type(self))

    async def upsert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        raise NotImplementedError(type(self))

    async def overwrite(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        from databricks.feature_engineering import FeatureEngineeringClient

        client = FeatureEngineeringClient()

        conn = self.config.connection()
        df = conn.createDataFrame(await job.to_pandas())

        client.create_table(
            name=self.table.identifier(),
            primary_keys=list(request.entity_names),
            df=df
        )


@dataclass
class UCTableSource(BatchDataSource):

    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig

    def job_group_key(self) -> str:
        return f"uc_table-{self.table.identifier()}"

    def all_data(self, request: RetrivalRequest, limit: int | None) -> RetrivalJob:

        async def load() -> pl.LazyFrame:
            con = self.config.connection()
            spark_df = con.read.table(self.table.identifier())

            if limit:
                spark_df = spark_df.limit(limit)

            return pl.from_pandas(spark_df.toPandas()).lazy()

        return RetrivalJob.from_lazy_function(load, request)

    def all_between_dates(
        self,
        request: RetrivalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrivalJob:

        raise NotImplementedError(type(self))

    @classmethod
    def multi_source_features_for(
        cls, facts: RetrivalJob, requests: list[tuple['UCTableSource', RetrivalRequest]] # noqa: ANN102
    ) -> RetrivalJob:
        from aligned.sources.local import DateFormatter

        if len(requests) != 1:
            raise NotImplementedError(
                f'Type: {cls} have not implemented how to load fact data with multiple sources.'
            )

        source, request = requests[0]
        spark = source.config.connection()

        async def load() -> pl.LazyFrame:
            df = spark.read.table(source.table.identifier())
            return pl.from_pandas(df.toPandas()).lazy()

        return FileFactualJob(
            source=RetrivalJob.from_lazy_function(load, request),
            date_formatter=DateFormatter.noop(),
            requests=[request],
            facts=facts,
        )


    def features_for(self, facts: RetrivalJob, request: RetrivalRequest) -> RetrivalJob:
        return type(self).multi_source_features_for(facts, [(self, request)])

    async def schema(self) -> dict[str, FeatureType]:
        """Returns the schema for the data source

        ```python
        source = FileSource.parquet_at('test_data/titanic.parquet')
        schema = await source.schema()
        >>> {'passenger_id': FeatureType(name='int64'), ...}
        ```

        Returns:
            dict[str, FeatureType]: A dictionary containing the column name and the feature type
        """
        raise NotImplementedError(f'`schema()` is not implemented for {type(self)}.')

    async def freshness(self, event_timestamp: EventTimestamp) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        spark = self.config.connection()
        return spark.sql(
            f"SELECT MAX({event_timestamp.name}) as {event_timestamp.name} FROM {self.table.identifier()}"
        ).toPandas()[event_timestamp.name].to_list()[0]
