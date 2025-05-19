from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from aligned.data_source.batch_data_source import (
    BatchDataSource,
    CodableBatchDataSource,
    FeatureType,
    RequestResult,
)
from aligned.feature_source import WritableFeatureSource
from aligned.retrieval_job import RetrievalJob, RetrievalRequest
from aligned.schemas.constraints import MaxLength, MinLength
from aligned.schemas.feature import Constraint, Feature
from aligned.sources.local import FileFactualJob

from data_contracts.config_values import EnvironmentValue, LiteralValue, ValueRepresentable
from data_contracts.helper import snake_to_pascal

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import DataType, StructField, StructType


def is_running_on_databricks() -> bool:
    import os

    return "DATABRICKS_RUNTIME_VERSION" in os.environ


logger = logging.getLogger(__name__)


@dataclass
class DatabricksAuthConfig:
    token: str
    host: str


@dataclass
class SchemaChange:
    adding: list[StructField]
    changes: dict[str, SchemaChange | StructField]
    deletes: list[StructField]

    @property
    def has_changes(self) -> bool:
        if self.adding or self.deletes:
            return True
        return bool(self.changes)

    def to_spark_sql_struct(self, column: str, table_identifier: str) -> str:
        raw_sql = ""

        alter_table = f"ALTER TABLE {table_identifier}"

        for add in self.adding:
            raw_sql += f"{alter_table} ADD COLUMN {column}.{add.name} {add.dataType.simpleString()};\n"

        for spark_field in self.changes.values():
            assert not isinstance(spark_field, SchemaChange)
            raw_sql += f"{alter_table} DROP COLUMN {column}.{spark_field.name};\n"
            raw_sql += f"{alter_table} ADD COLUMN {column}.{spark_field.name} {spark_field.dataType.simpleString()};\n"

        return raw_sql

    def to_spark_sql(self, table_identifier: str) -> str:
        raw_sql = ""
        alter_table = f"ALTER TABLE {table_identifier}"

        for add in self.adding:
            raw_sql += f"{alter_table} ADD COLUMN {add.name} {add.dataType.simpleString()};\n"

        for name, changes in self.changes.items():
            if isinstance(changes, SchemaChange):
                raw_sql += changes.to_spark_sql_struct(name, table_identifier)
            else:
                raw_sql += f"{alter_table} ALTER COLUMN {name} TYPE {changes.dataType.simpleString()};\n"

        return raw_sql


def pyspark_schema_changes(from_schema: StructType, to_schema: StructType) -> SchemaChange:
    from pyspark.sql.types import ArrayType, StructType

    from_map = {field.name: field for field in from_schema.fields}
    to_map = {field.name: field for field in to_schema.fields}

    new_fields: list[StructField] = []
    alter_field = {}
    deletes: list[StructField] = []

    for new_field in to_map.values():
        if new_field.name not in from_map:
            new_fields.append(new_field)
            continue

        existing_field = from_map[new_field.name]

        if isinstance(new_field.dataType, StructType) and isinstance(existing_field.dataType, StructType):
            changes = pyspark_schema_changes(existing_field.dataType, new_field.dataType)
            if changes.has_changes:
                alter_field[new_field.name] = changes

        elif isinstance(new_field.dataType, ArrayType) and isinstance(existing_field.dataType, ArrayType):
            new_sub_el = new_field.dataType.elementType
            old_sub_el = existing_field.dataType.elementType

            if isinstance(new_sub_el, StructType) and isinstance(old_sub_el, StructType):
                changes = pyspark_schema_changes(old_sub_el, new_sub_el)
                if changes.has_changes:
                    alter_field[new_field.name] = changes

            elif new_sub_el != old_sub_el:
                alter_field[new_field.name] = new_field

        elif new_field.dataType != existing_field.dataType:
            to_type = convert_pyspark_type(new_field.dataType)
            from_type = convert_pyspark_type(existing_field.dataType)

            if to_type.dtype.is_numeric and from_type.dtype.is_numeric:
                continue

            alter_field[new_field.name] = new_field

    for from_field in from_map.values():
        if from_field.name not in to_map:
            deletes.append(from_field)

    return SchemaChange(adding=new_fields, changes=alter_field, deletes=deletes)


def pyspark_schema_from_request(request: RetrievalRequest) -> StructType:
    from pyspark.sql.types import StructField, StructType

    def spark_type(dtype: FeatureType) -> DataType:
        if dtype.is_struct and dtype.has_structured_fields:
            sub_fields = dtype.struct_fields()
            return StructType(
                [
                    StructField(name=key, dataType=dtype.spark_type)
                    for key, dtype in sorted(sub_fields.items(), key=lambda field: field[0])
                ]
            )
        return dtype.spark_type

    return StructType(
        [
            StructField(name=feature.name, dataType=spark_type(feature.dtype))
            for feature in sorted(request.all_returned_features, key=lambda feat: feat.name)
        ]
    )


def raise_on_invalid_pyspark_schema(schema: DataType) -> None:
    from pyspark.sql.types import ArrayType, NullType, StructType

    if isinstance(schema, StructType):
        for field in schema.fields:
            raise_on_invalid_pyspark_schema(field)

    if isinstance(schema, ArrayType):
        raise_on_invalid_pyspark_schema(schema.elementType)

    if isinstance(schema, NullType):
        raise ValueError("Found a NullType in the schema. This will lead to issues.")


@dataclass
class SparkDataType:
    dtype: FeatureType
    constraints: list[Constraint]


def convert_pyspark_type(data_type: DataType) -> SparkDataType:  # noqa: PLR0911
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        ByteType,
        CharType,
        DateType,
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
        VarcharType,
    )

    def no_constraints(dtype: FeatureType) -> SparkDataType:
        return SparkDataType(dtype, [])

    if isinstance(data_type, VarcharType):
        return SparkDataType(
            FeatureType.string(),
            [
                MaxLength(data_type.length),
            ],
        )
    if isinstance(data_type, CharType):
        return SparkDataType(
            FeatureType.string(),
            [
                MinLength(data_type.length),
                MaxLength(data_type.length),
            ],
        )
    if isinstance(data_type, StringType):
        return no_constraints(FeatureType.string())
    if isinstance(data_type, FloatType):
        return no_constraints(FeatureType.floating_point())
    if isinstance(data_type, DoubleType):
        return no_constraints(FeatureType.double())
    if isinstance(data_type, ByteType):
        return no_constraints(FeatureType.int8())
    if isinstance(data_type, ShortType):
        return no_constraints(FeatureType.int16())
    if isinstance(data_type, IntegerType):
        return no_constraints(FeatureType.int32())
    if isinstance(data_type, LongType):
        return no_constraints(FeatureType.int64())
    if isinstance(data_type, BooleanType):
        return no_constraints(FeatureType.boolean())
    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return no_constraints(FeatureType.datetime())
    if isinstance(data_type, DateType):
        return no_constraints(FeatureType.date())
    if isinstance(data_type, ArrayType):
        return no_constraints(FeatureType.array(convert_pyspark_type(data_type.elementType).dtype))
    if isinstance(data_type, StructType):
        return no_constraints(FeatureType.json())
    if isinstance(data_type, MapType):
        return no_constraints(FeatureType.json())

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
        token: str | ValueRepresentable | None,
    ) -> None:
        self.host = LiteralValue.from_value(host)
        self.cluster_id = LiteralValue.from_value(cluster_id) if isinstance(cluster_id, str) else cluster_id
        self.token = LiteralValue(token) if isinstance(token, str) else token

    def with_auth(self, token: str | ValueRepresentable, host: str | ValueRepresentable) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(cluster_id=self.cluster_id, token=token, host=host)

    @staticmethod
    def databricks_or_serverless(
        host: str | ValueRepresentable | None = None, token: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(
            cluster_id=None,
            token=token or EnvironmentValue("DATABRICKS_TOKEN"),
            host=host or EnvironmentValue("DATABRICKS_HOST"),
        )

    @staticmethod
    def serverless(
        host: str | ValueRepresentable | None = None, token: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(
            cluster_id="serverless",
            token=token or EnvironmentValue("DATABRICKS_TOKEN"),
            host=host or EnvironmentValue("DATABRICKS_HOST"),
        )

    @staticmethod
    def with_cluster_id(
        cluster_id: str | ValueRepresentable, host: str | ValueRepresentable
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(cluster_id=cluster_id, token=None, host=host)

    def catalog(self, catalog: str | ValueRepresentable) -> UnityCatalog:
        return UnityCatalog(self, LiteralValue.from_value(catalog))

    def sql_file(self, file_path: str | Path, format_values: dict[str, str] | None = None) -> UCSqlSource:
        """
        Creates a source that runs a sql query based on a file path.

        Args:
            file_path (str | Path): The path to read the sql from

        Returns:
            UCSqlSource: A source that runs an sql file
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)

        content = file_path.read_text()

        if format_values:
            content = content.format(**format_values)

        return UCSqlSource(self, content)

    def connection(self) -> SparkSession:
        from pyspark.errors import PySparkException

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

        if cluster_id_value == "serverless":
            spark = builder.getOrCreate()
            try:
                spark.sql("SELECT 1")
                return spark
            except PySparkException:
                spark.stop()

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

    async def list_tables(self) -> list[str]:
        con = self.config.connection()
        tables = con.sql(f"SHOW TABLES {self.catalog.read()}.{self.schema.read()};").toPandas()
        return tables["tableName"].to_list()

    def table(self, table: str | ValueRepresentable) -> UCTableSource:
        return UCTableSource(
            self.config, UnityCatalogTableConfig(self.catalog, self.schema, LiteralValue.from_value(table))
        )

    def feature_table(self, table: str | ValueRepresentable) -> UCFeatureTableSource:
        return UCFeatureTableSource(
            self.config, UnityCatalogTableConfig(self.catalog, self.schema, LiteralValue.from_value(table))
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

    async def to_pandas(self) -> pd.DataFrame:
        return await self.all_columns().to_pandas()

    async def to_polars(self) -> pl.DataFrame:
        return await self.all_columns().to_polars()

    def all_data(self, request: RetrievalRequest, limit: int | None) -> RetrievalJob:
        client = self.config.connection()

        async def load() -> pl.LazyFrame:
            spark_df = client.sql(self.query)

            if limit:
                spark_df = spark_df.limit(limit)

            return pl.from_pandas(spark_df.toPandas()).lazy()

        return RetrievalJob.from_lazy_function(load, request)

    def all_between_dates(
        self,
        request: RetrievalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        raise NotImplementedError(type(self))

    @classmethod
    def multi_source_features_for(  # type: ignore
        cls: type[UCSqlSource], facts: RetrievalJob, requests: list[tuple[UCSqlSource, RetrievalRequest]]
    ) -> RetrievalJob:
        raise NotImplementedError(cls)

    def features_for(self, facts: RetrievalJob, request: RetrievalRequest) -> RetrievalJob:
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
        raise NotImplementedError(f"`schema()` is not implemented for {type(self)}.")

    async def freshness(self, feature: Feature) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        raise NotImplementedError(type(self))

    def with_config(self, config: DatabricksConnectionConfig) -> UCSqlSource:
        return UCSqlSource(config, self.query)


@dataclass
class UCFeatureTableSource(CodableBatchDataSource, WritableFeatureSource, DatabricksSource):
    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig

    type_name = "uc_feature_table"

    def job_group_key(self) -> str:
        return "uc_feature_table"

    def all_data(self, request: RetrievalRequest, limit: int | None) -> RetrievalJob:
        from databricks.feature_engineering import FeatureEngineeringClient

        client = FeatureEngineeringClient()

        async def load() -> pl.LazyFrame:
            spark_df = client.read_table(name=self.table.identifier())

            if limit:
                spark_df = spark_df.limit(limit)

            return pl.from_pandas(spark_df.toPandas()).lazy()

        return RetrievalJob.from_lazy_function(load, request)

    def all_between_dates(
        self,
        request: RetrievalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        raise NotImplementedError(type(self))

    @classmethod
    def multi_source_features_for(  # type: ignore
        cls: type[UCFeatureTableSource],
        facts: RetrievalJob,
        requests: list[tuple[UCFeatureTableSource, RetrievalRequest]],
    ) -> RetrievalJob:
        from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

        keys = {source.job_group_key() for source, _ in requests if isinstance(source, BatchDataSource)}
        if len(keys) != 1:
            raise NotImplementedError(f"Type: {cls} have not implemented how to load fact data with multiple sources.")

        client = FeatureEngineeringClient()

        result_request: RetrievalRequest | None = None
        lookups = []

        for source, request in requests:
            lookups.append(
                FeatureLookup(
                    source.table.identifier(),
                    lookup_key=list(request.entity_names),
                    feature_names=request.feature_names,
                    timestamp_lookup_key=request.event_timestamp.name if request.event_timestamp else None,
                )
            )

            if result_request is None:
                result_request = request
            else:
                result_request = result_request.unsafe_combine([request])

        assert lookups, "Found no lookups"
        assert result_request, "A `request_result` was supposed to be created."

        async def load() -> pl.LazyFrame:
            import pyspark.pandas as ps

            df = await facts.to_pandas()

            dataset = client.create_training_set(
                df=ps.from_pandas(df),  # type: ignore
                feature_lookups=lookups,
                label=None,
                exclude_columns=None,
            )

            return pl.from_pandas(dataset.load_df().toPandas()).lazy()

        return RetrievalJob.from_lazy_function(load, result_request)

    def features_for(self, facts: RetrievalJob, request: RetrievalRequest) -> RetrievalJob:
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
        raise NotImplementedError(f"`schema()` is not implemented for {type(self)}.")

    async def freshness(self, feature: Feature) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        spark = self.config.connection()
        return (
            spark.sql(f"SELECT MAX({feature.name}) as {feature.name} FROM {self.table.identifier()}")
            .toPandas()[feature.name]
            .to_list()[0]
        )

    async def insert(self, job: RetrievalJob, request: RetrievalRequest) -> None:
        raise NotImplementedError(type(self))

    async def upsert(self, job: RetrievalJob, request: RetrievalRequest) -> None:
        raise NotImplementedError(type(self))

    async def overwrite(self, job: RetrievalJob, request: RetrievalRequest) -> None:
        from databricks.feature_engineering import FeatureEngineeringClient

        client = FeatureEngineeringClient()

        conn = self.config.connection()
        df = conn.createDataFrame(await job.unique_entities().to_pandas())

        client.create_table(name=self.table.identifier(), primary_keys=list(request.entity_names), df=df)

    def with_config(self, config: DatabricksConnectionConfig) -> UCFeatureTableSource:
        return UCFeatureTableSource(config, self.table)


def features_to_read(request: RetrievalRequest, schema: StructType) -> list[str]:
    stored_fields = schema.fieldNames()

    columns = list(request.entity_names)

    for feat in request.all_returned_features:
        if feat.name in stored_fields:
            columns.append(feat.name)
        elif not feat.default_value:
            raise ValueError(
                f"Missing column {feat.name}. Either add it to the table {request.location}, or add a default value."
                f"Available schema is {schema}"
            )

    if request.event_timestamp:
        columns.append(request.event_timestamp.name)
    return list(set(columns))


def validate_pyspark_schema(old: StructType, new: StructType, sub_property: str = "") -> None:
    from pyspark.sql.types import ArrayType, StructType

    old_schema = {field.name: field.dataType for field in old.fields}
    new_schema = {field.name: field.dataType for field in new.fields}

    missing_fields = []
    incorrect_schema = []

    for name, dtype in old_schema.items():
        if name not in new_schema:
            missing_fields.append((name, dtype))
        elif dtype != new_schema[name]:
            new_type = new_schema[name]
            if isinstance(dtype, StructType) and isinstance(new_type, StructType):
                validate_pyspark_schema(dtype, new_type, f"{sub_property}.{name}." if sub_property else name)
            elif (
                isinstance(dtype, ArrayType)
                and isinstance(new_type, ArrayType)
                and isinstance(dtype.elementType, StructType)
                and isinstance(new_type.elementType, StructType)
            ):
                validate_pyspark_schema(
                    dtype.elementType, new_type.elementType, f"{sub_property}.{name}." if sub_property else name
                )
            else:
                both_are_numeric = (
                    convert_pyspark_type(dtype).dtype.is_numeric and convert_pyspark_type(new_type).dtype.is_numeric
                )
                if not both_are_numeric:
                    incorrect_schema.append((name, dtype, new_schema[name]))

    error_message = ""

    if missing_fields:
        error_message += "\n".join(
            [f"Missing column '{sub_property}.{name}' with data type: {dtype}" for name, dtype in missing_fields]
        )

    if incorrect_schema:
        error_message += "\n".join(
            [
                f"Incorrect schema for '{sub_property}.{name}' got {new_dtype}, but expected {old_dtype}"
                for name, old_dtype, new_dtype in incorrect_schema
            ]
        )

    if error_message:
        raise ValueError(error_message)


@dataclass
class UnityCatalogTableAllJob(RetrievalJob):
    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig
    request: RetrievalRequest
    limit: int | None
    where: str | None = field(default=None)

    @property
    def request_result(self) -> RequestResult:
        return self.request.request_result

    @property
    def retrieval_requests(self) -> list[RetrievalRequest]:
        return [self.request]

    def filter(self, condition: str | Feature | pl.Expr) -> RetrievalJob:
        if isinstance(condition, Feature):
            new_where = condition.name
        elif isinstance(condition, str):
            new_where = condition
        elif isinstance(condition, pl.Expr):
            from data_contracts.polars_to_spark import polars_expression_to_spark

            spark_expr = polars_expression_to_spark(condition)
            if not spark_expr:
                return RetrievalJob.filter(self, condition)

            new_where = spark_expr

        if self.where:
            self.where = f"({self.where}) AND ({new_where})"
        else:
            self.where = new_where

        return self

    async def to_pyspark(self) -> DataFrame:
        con = self.config.connection()
        spark_df = con.read.table(self.table.identifier())

        if self.request.features_to_include:
            spark_df = spark_df.select(features_to_read(self.request, spark_df.schema))

        if self.where:
            spark_df = spark_df.filter(self.where)

        if self.limit:
            spark_df = spark_df.limit(self.limit)

        return spark_df

    async def to_pandas(self) -> pd.DataFrame:
        from pyspark.sql.functions import lit

        spark_df = await self.to_pyspark()

        for feat in self.request.features:
            if feat.name not in spark_df.columns and feat.default_value:
                spark_df = spark_df.withColumn(feat.name, lit(feat.default_value.python_value))

        return spark_df.toPandas()

    async def to_lazy_polars(self) -> pl.LazyFrame:
        return pl.from_pandas(
            await self.to_pandas(),
            schema_overrides={
                feat.name: feat.dtype.polars_type
                for feat in self.retrieval_requests[0].features
                if feat.dtype != FeatureType.json()
            },
        ).lazy()


@dataclass
class UCTableSource(CodableBatchDataSource, WritableFeatureSource, DatabricksSource):
    """
    A source that connects to a Databricks Unity Catalog table
    """

    config: DatabricksConnectionConfig
    table: UnityCatalogTableConfig
    should_overwrite_schema: bool = False

    type_name = "uc_table"

    def job_group_key(self) -> str:
        # One fetch job per table
        return f"uc_table-{self.table.identifier()}"

    def overwrite_schema(self, should_overwrite_schema: bool = True) -> UCTableSource:
        return UCTableSource(config=self.config, table=self.table, should_overwrite_schema=should_overwrite_schema)

    def all_data(self, request: RetrievalRequest, limit: int | None) -> RetrievalJob:
        return UnityCatalogTableAllJob(self.config, self.table, request, limit)

    def all_between_dates(
        self,
        request: RetrievalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        raise NotImplementedError(type(self))

    @classmethod
    def multi_source_features_for(  # type: ignore
        cls: type[UCTableSource],
        facts: RetrievalJob,
        requests: list[tuple[UCTableSource, RetrievalRequest]],  # type: ignore
    ) -> RetrievalJob:
        from aligned.sources.local import DateFormatter

        if len(requests) != 1:
            raise NotImplementedError(f"Type: {cls} have not implemented how to load fact data with multiple sources.")

        source, request = requests[0]
        spark = source.config.connection()

        async def load() -> pl.LazyFrame:
            df = spark.read.table(source.table.identifier())
            df = df.select(features_to_read(request, df.schema))
            return pl.from_pandas(df.toPandas()).lazy()

        return FileFactualJob(
            source=RetrievalJob.from_lazy_function(load, request),
            date_formatter=DateFormatter.noop(),
            requests=[request],
            facts=facts,
        )

    def features_for(self, facts: RetrievalJob, request: RetrievalRequest) -> RetrievalJob:
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

        for column in schema.fields:
            aligned_schema[column.name] = convert_pyspark_type(column.dataType).dtype
        return aligned_schema

    async def freshness(self, feature: Feature) -> datetime | None:
        """
        my_table_freshenss = await (PostgreSQLConfig("DB_URL")
            .table("my_table")
            .freshness()
        )
        """
        spark = self.config.connection()
        return (
            spark.sql(f"SELECT MAX({feature.name}) as {feature.name} FROM {self.table.identifier()}")
            .toPandas()[feature.name]
            .to_list()[0]
        )

    async def insert(self, job: RetrievalJob, request: RetrievalRequest) -> None:
        pdf = (await job.to_polars()).select(request.all_returned_columns)
        schema = pyspark_schema_from_request(request)

        conn = self.config.connection()
        df = conn.createDataFrame(
            pdf.to_pandas(),
            schema=schema,  # type: ignore
        )
        if conn.catalog.tableExists(self.table.identifier()):
            schema = conn.table(self.table.identifier()).schema
        validate_pyspark_schema(old=schema, new=df.schema)
        df.write.mode("append").saveAsTable(self.table.identifier())

    async def upsert(self, job: RetrievalJob, request: RetrievalRequest) -> None:
        pdf = (await job.unique_entities().to_polars()).select(request.all_returned_columns)

        target_table = self.table.identifier()
        conn = self.config.connection()

        if not conn.catalog.tableExists(target_table):
            await self.insert(job, request)
        else:
            entities = request.entity_names
            on_statement = " AND ".join([f"target.{ent} = source.{ent}" for ent in entities])

            schema = pyspark_schema_from_request(request)
            df = conn.createDataFrame(pdf.to_pandas(), schema=schema)
            schema = conn.table(target_table).schema
            validate_pyspark_schema(old=schema, new=df.schema)

            temp_table = "new_values"
            df.createOrReplaceTempView(temp_table)
            conn.sql(f"""MERGE INTO {target_table} AS target
USING {temp_table} AS source
ON {on_statement}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *""")

    async def overwrite(self, job: RetrievalJob, request: RetrievalRequest) -> None:
        pdf = (await job.to_polars()).select(request.all_returned_columns)
        schema = pyspark_schema_from_request(request)
        raise_on_invalid_pyspark_schema(schema)
        conn = self.config.connection()
        df = conn.createDataFrame(pdf.to_pandas(), schema=schema)
        df.write.mode("overwrite").option("overwriteSchema", self.should_overwrite_schema).saveAsTable(
            self.table.identifier()
        )

    def with_config(self, config: DatabricksConnectionConfig) -> UCTableSource:
        return UCTableSource(config, self.table)

    async def feature_view_code(self, view_name: str) -> str:
        from pyspark.sql.types import DataType

        con = self.config.connection()
        columns = con.sql(f"DESCRIBE TABLE {self.table.identifier()}").toPandas()

        source = f"{self.config}"

        if isinstance(self.table.catalog, LiteralValue):
            source += f".catalog('{self.table.catalog.read()}')"
        else:
            source += f".catalog({self.table.catalog})"

        if isinstance(self.table.schema, LiteralValue):
            source += f".schema('{self.table.schema.read()}')"
        else:
            source += f".schema({self.table.schema})"

        if isinstance(self.table.table, LiteralValue):
            source += f".table('{self.table.table.read()}')"
        else:
            source += f".table({self.table.table})"

        uppercased_name = snake_to_pascal(view_name)

        data_types: set[str] = set()
        feature_code = ""
        for row in columns.sort_values("col_name").to_dict(orient="records"):
            name = row["col_name"]
            comment = row["comment"]

            spark_type = convert_pyspark_type(DataType.fromDDL(row["data_type"]))
            dtype = spark_type.dtype.feature_factory

            type_name = dtype.__class__.__name__
            data_types.add(type_name)
            feature_code += f"{name} = {type_name}()"

            for constraint in spark_type.constraints:
                if isinstance(constraint, MaxLength):
                    feature_code += f".max_length({constraint.value})"
                elif isinstance(constraint, MinLength):
                    feature_code += f".min_length({constraint.value})"

            if comment:
                formatted_comment = comment.replace("\n", "\\n")
                feature_code += f'\n    "{formatted_comment}"\n'

            feature_code += "\n    "

        all_types = ", ".join(data_types)

        return f"""from aligned import feature_view, {all_types}
from data_contracts.unity_catalog import DatabricksConnectionConfig

@feature_view(
    name="{view_name}",
    description="A databricks table containing {view_name}",
    source={source},
    tags=['code-generated', 'databricks']
)
class {uppercased_name}:
    {feature_code}"""
