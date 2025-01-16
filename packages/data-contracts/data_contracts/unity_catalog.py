from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl
from aligned.data_source.batch_data_source import (
    BatchDataSource,
    CodableBatchDataSource,
    FeatureType,
)
from aligned.feature_source import WritableFeatureSource
from aligned.retrival_job import RetrivalJob, RetrivalRequest
from aligned.schemas.feature import Feature
from aligned.sources.local import FileFactualJob

from data_contracts.config_values import EnvironmentValue, LiteralValue, ValueRepresentable

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DataType, StructField, StructType


def is_running_on_databricks() -> bool:
    import os
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

logger = logging.getLogger(__name__)

@dataclass
class DatabricksAuthConfig:
    token: str
    host: str

def polars_schema_to_spark(schema: dict[str, pl.PolarsDataType]) -> StructType:
    from pyspark.sql.types import StructField, StructType

    return StructType([
        StructField(
            name=name,
            dataType=polars_dtype_to_spark(dtype)
        )
        for name, dtype in schema.items()
    ])


def polars_dtype_to_spark(data_type: pl.PolarsDataType) -> DataType:  # noqa: PLR0911
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        ByteType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        StructType,
        TimestampType,
    )

    if isinstance(data_type, pl.String):
        return StringType()
    if isinstance(data_type, pl.Float32):
        return FloatType()
    if isinstance(data_type, pl.Float64):
        return DoubleType()
    if isinstance(data_type, pl.Int8):
        return ByteType()
    if isinstance(data_type, pl.Int16):
        return ShortType()
    if isinstance(data_type, pl.Int32):
        return IntegerType()
    if isinstance(data_type, pl.Int64):
        return LongType()
    if isinstance(data_type, pl.Boolean):
        return BooleanType()
    if isinstance(data_type, pl.Datetime):
        return TimestampType()
    if isinstance(data_type, (pl.Array, pl.List)):
        if data_type.inner:
            return ArrayType(
                polars_dtype_to_spark(data_type.inner)
            )
        return ArrayType(StringType())
    if isinstance(data_type, pl.Struct):
        return StructType([
            StructField(
                name=field.name,
                dataType=polars_dtype_to_spark(field.dtype)
            )
            for field in data_type.fields
        ])

    raise ValueError(f"Unsupported type {data_type}")



def convert_pyspark_type(data_type: DataType) -> FeatureType:  # noqa: PLR0911
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        ByteType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        MapType,
        ShortType,
        StringType,
        StructType,
        TimestampNTZType,
        TimestampType,
    )

    if isinstance(data_type, StringType):
        return FeatureType.string()
    if isinstance(data_type, FloatType):
        return FeatureType.floating_point()
    if isinstance(data_type, DoubleType):
        return FeatureType.double()
    if isinstance(data_type, ByteType):
        return FeatureType.int8()
    if isinstance(data_type, ShortType):
        return FeatureType.int16()
    if isinstance(data_type, IntegerType):
        return FeatureType.int32()
    if isinstance(data_type, LongType):
        return FeatureType.int64()
    if isinstance(data_type, BooleanType):
        return FeatureType.boolean()
    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return FeatureType.datetime()
    if isinstance(data_type, ArrayType):
        return FeatureType.array(
            convert_pyspark_type(data_type.elementType)
        )
    if isinstance(data_type, StructType):
        return FeatureType.json()
    if isinstance(data_type, MapType):
        return FeatureType.json()

    raise ValueError(f"Unsupported type {data_type}")


@dataclass(init=False)
class DatabricksConnectionConfig:

    host: ValueRepresentable
    cluster_id: ValueRepresentable | None
    token: ValueRepresentable | None

    def __init__(
        self,
        host: str | ValueRepresentable,
        cluster_id: str | ValueRepresentable | None,
        token: str | ValueRepresentable | None
    ) -> None:
        self.host = LiteralValue.from_value(host)
        self.cluster_id = LiteralValue.from_value(cluster_id) if isinstance(cluster_id, str) else cluster_id
        self.token = LiteralValue(token) if isinstance(token, str) else token

    def with_auth(
        self,
        token: str | ValueRepresentable,
        host: str | ValueRepresentable
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(
            cluster_id=self.cluster_id,
            token=token,
            host=host
        )

    @staticmethod
    def databricks_or_serverless(
        host: str | ValueRepresentable | None = None,
        token: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(
            cluster_id=None,
            token=token or EnvironmentValue("DATABRICKS_TOKEN"),
            host=host or EnvironmentValue("DATABRICKS_HOST")
        )

    @staticmethod
    def serverless(
        host: str | ValueRepresentable | None = None,
        token: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(
            cluster_id="serverless",
            token=token or EnvironmentValue("DATABRICKS_TOKEN"),
            host=host or EnvironmentValue("DATABRICKS_HOST")
        )

    @staticmethod
    def with_cluster_id(
        cluster_id: str | ValueRepresentable,
        host: str | ValueRepresentable
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(
            cluster_id=cluster_id, token=None, host=host
        )

    def catalog(self, catalog: str | ValueRepresentable) -> UnityCatalog:
        return UnityCatalog(self, LiteralValue.from_value(catalog))

    def connection(self) -> SparkSession:

        cluster_id = self.cluster_id

        if not cluster_id:
            from databricks.sdk.runtime import spark

            if spark is not None:
                return spark

            # If no spark session
            # Assume that serverless is available
            cluster_id = LiteralValue("serverless")

        from databricks.connect.session import DatabricksSession

        builder = DatabricksSession.builder.host(self.host.read())

        cluster_id_value = cluster_id.read()
        if cluster_id_value == "serverless":
            builder = builder.serverless()
        else:
            builder = builder.clusterId(cluster_id_value)

        if self.token:
            builder = builder.token(self.token.read())

        return builder.getOrCreate()

    def sql(self, query: str) -> UCSqlSource:
        return UCSqlSource(self, query)

@dataclass
class UnityCatalog:
    config: DatabricksConnectionConfig

    catalog: ValueRepresentable

    def schema(self, schema: str | ValueRepresentable) -> UnityCatalogSchema:
        return UnityCatalogSchema(self.config, self.catalog, LiteralValue.from_value(schema))

    def sql(self, query: str) -> UCSqlSource:
        return UCSqlSource(self.config, query)


@dataclass
class UnityCatalogSchema:
    config: DatabricksConnectionConfig

    catalog: ValueRepresentable
    schema: ValueRepresentable

    def table(self, table: str | ValueRepresentable) -> UCTableSource:
        return UCTableSource(
            self.config,
            UnityCatalogTableConfig(self.catalog, self.schema, LiteralValue.from_value(table))
        )

    def feature_table(self, table: str | ValueRepresentable) -> UCFeatureTableSource:
        return UCFeatureTableSource(
            self.config,
            UnityCatalogTableConfig(self.catalog, self.schema, LiteralValue.from_value(table))
        )


@dataclass
class UnityCatalogTableConfig:
    catalog: ValueRepresentable
    schema: ValueRepresentable
    table: ValueRepresentable

    def identifier(self) -> str:
        return f"{self.catalog.read()}.{self.schema.read()}.{self.table.read()}"


class DatabricksSource:
    config: DatabricksConnectionConfig


@dataclass
class UCSqlSource(CodableBatchDataSource, DatabricksSource):

    config: DatabricksConnectionConfig
    query: str

    type_name = "uc_sql"

    def all_data(self, request: RetrivalRequest, limit: int | None) -> RetrivalJob:
        client = self.config.connection()

        async def load() -> pl.LazyFrame:
            spark_df = client.sql(self.query)

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
    def multi_source_features_for( # type: ignore
        cls: type[UCSqlSource],
        facts: RetrivalJob,
        requests: list[tuple[UCSqlSource, RetrivalRequest]]
    ) -> RetrivalJob:
        raise NotImplementedError(cls)


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

    async def freshness(self, feature: Feature) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        raise NotImplementedError(type(self))

    def with_config(self, config: DatabricksConnectionConfig) -> UCSqlSource:
        return UCSqlSource(
            config, self.query
        )



@dataclass
class UCFeatureTableSource(CodableBatchDataSource, WritableFeatureSource, DatabricksSource):

    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig

    type_name = "uc_feature_table"

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
    def multi_source_features_for( # type: ignore
        cls: type[UCFeatureTableSource],
        facts: RetrivalJob,
        requests: list[tuple[UCFeatureTableSource, RetrivalRequest]]
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
                result_request  = result_request.unsafe_combine([request])

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

    async def freshness(self, feature: Feature) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        spark = self.config.connection()
        return spark.sql(
            f"SELECT MAX({feature.name}) as {feature.name} FROM {self.table.identifier()}"
        ).toPandas()[feature.name].to_list()[0]


    async def insert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        raise NotImplementedError(type(self))

    async def upsert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        raise NotImplementedError(type(self))

    async def overwrite(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        from databricks.feature_engineering import FeatureEngineeringClient

        client = FeatureEngineeringClient()

        conn = self.config.connection()
        df = conn.createDataFrame(await job.unique_entities().to_pandas())

        client.create_table(
            name=self.table.identifier(),
            primary_keys=list(request.entity_names),
            df=df
        )

    def with_config(self, config: DatabricksConnectionConfig) -> UCFeatureTableSource:
        return UCFeatureTableSource(config, self.table)

def features_to_read(request: RetrivalRequest) -> list[str]:
    columns = list(request.all_required_feature_names.union(request.entity_names))
    if request.event_timestamp:
        columns.append(request.event_timestamp.name)
    return columns

@dataclass
class UCTableSource(CodableBatchDataSource, WritableFeatureSource, DatabricksSource):

    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig

    type_name = "uc_table"

    def job_group_key(self) -> str:
        # One fetch job per table
        return f"uc_table-{self.table.identifier()}"

    def all_data(self, request: RetrivalRequest, limit: int | None) -> RetrivalJob:

        async def load() -> pl.LazyFrame:
            con = self.config.connection()
            spark_df = con.read.table(self.table.identifier()).select(
                features_to_read(request)
            )

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
    def multi_source_features_for( # type: ignore
        cls: type[UCTableSource], facts: RetrivalJob, requests: list[tuple[UCTableSource, RetrivalRequest]] # type: ignore
    ) -> RetrivalJob:
        from aligned.sources.local import DateFormatter

        if len(requests) != 1:
            raise NotImplementedError(
                f'Type: {cls} have not implemented how to load fact data with multiple sources.'
            )

        source, request = requests[0]
        spark = source.config.connection()

        async def load() -> pl.LazyFrame:
            df = spark.read.table(source.table.identifier()).select(
                features_to_read(request)
            )
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
        spark = self.config.connection()
        schema = spark.table(self.table.identifier()).schema

        aligned_schema: dict[str, FeatureType] = {}

        for field in schema.fields:
            aligned_schema[field.name] = convert_pyspark_type(field.dataType)
        return aligned_schema

    async def freshness(self, feature: Feature) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        spark = self.config.connection()
        return spark.sql(
            f"SELECT MAX({feature.name}) as {feature.name} FROM {self.table.identifier()}"
        ).toPandas()[feature.name].to_list()[0]

    async def insert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        pdf = await job.to_polars()
        schema = polars_schema_to_spark(pdf.schema)  # type: ignore

        conn = self.config.connection()
        df = conn.createDataFrame(
            pdf.to_pandas(),
            schema=schema # type: ignore
        )
        df.write.mode("append").saveAsTable(self.table.identifier())

    async def upsert(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        pdf = await job.unique_entities().to_polars()

        target_table = self.table.identifier()
        conn = self.config.connection()

        if not conn.catalog.tableExists(target_table):
            await self.insert(job, request)
        else:
            entities = request.entity_names
            on_statement = " AND ".join([
                f"target.{ent} = source.{ent}" for ent in entities
            ])
            df = conn.createDataFrame(
                pdf.to_pandas(),
                schema=polars_schema_to_spark(pdf.schema) # type: ignore
            )
            temp_table = "new_values"
            df.createOrReplaceTempView(temp_table)
            conn.sql(f"""MERGE INTO {target_table} AS target
USING {temp_table} AS source
ON {on_statement}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *""")


    async def overwrite(self, job: RetrivalJob, request: RetrivalRequest) -> None:
        pdf = await job.unique_entities().to_pandas()
        conn = self.config.connection()
        df = conn.createDataFrame(pdf)
        df.write.mode("overwrite").saveAsTable(self.table.identifier())

    def with_config(self, config: DatabricksConnectionConfig) -> UCTableSource:
        return UCTableSource(config, self.table)
