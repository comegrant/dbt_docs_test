from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import polars as pl
from aligned.data_source.batch_data_source import CodableBatchDataSource, ColumnFeatureMappable
from aligned.feature_source import WritableFeatureSource
from aligned.request.retrival_request import RetrivalRequest
from aligned.retrival_job import RequestResult, RetrivalJob
from aligned.schemas.codable import Codable
from aligned.schemas.derivied_feature import AggregatedFeature, AggregateOver

if TYPE_CHECKING:
    from aligned.schemas.feature import EventTimestamp

logger = logging.getLogger(__name__)


@dataclass
class SqlServerJob(RetrivalJob, CodableBatchDataSource):
    config: SqlServerConfig
    query: str
    requests: list[RetrivalRequest] = field(default_factory=list)
    type_name: str = "raw_sql_server_query"

    def job_group_key(self) -> str:
        return self.query

    @property
    def to_markdown(self) -> str:
        return f"""Type: *Sql Server Query*

Connection Env Var: `{self.config.env_var}`

Query:
```sql
{self.query}
```"""

    @property
    def request_result(self) -> RequestResult:
        return RequestResult.from_request_list(self.retrival_requests)

    @property
    def retrival_requests(self) -> list[RetrivalRequest]:
        return self.requests

    def join(
        self,
        job: RetrivalJob,
        method: str,
        on_columns: str | tuple[str, str],
    ) -> RetrivalJob:
        from aligned.retrival_job import JoinJobs

        if not isinstance(on_columns, str):
            left_on, right_on = on_columns
        else:
            left_on = on_columns
            right_on = on_columns

        if isinstance(job, SqlServerJob):
            return SqlServerJob(
                self.config,
                query=(
                    f"SELECT * FROM ({self.query}) as left {method} JOIN ({job.query})"
                    "as right ON left.{left_on} = right.{right_on}"
                ),
            )

        return JoinJobs(
            method=method, # type: ignore
            left_job=self,
            right_job=job,
            left_on=[left_on],
            right_on=[right_on],
        )

    async def to_lazy_polars(self) -> pl.LazyFrame:
        return pl.from_pandas(await self.to_pandas()).lazy()

    async def to_pandas(self) -> pd.DataFrame:
        return pd.read_sql(self.query, con=self.config.connection.raw_connection())

    def describe(self) -> str:
        return f"SqlServer Job: \n{self.query}\n"

    def all_between_dates(
        self,
        request: RetrivalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrivalJob:
        import sqlglot

        if not request.event_timestamp:
            raise ValueError(
                f"Unable to filter '{request.name}' features on datetimes without an event timestamp.",
            )

        event_column = request.event_timestamp.name

        parsed = sqlglot.parse_one(self.query)

        parsed = parsed.where(f"CONVERT(DATETIME, '{start_date}', 121) <= {event_column}", dialect="tsql").where(
            f"{event_column} <= CONVERT(DATETIME, '{end_date}', 121)", dialect="tsql"
        )
        query = parsed.sql(dialect="tsql")

        return SqlServerJob(self.config, query=query, requests=[request])

    def all_data(self, request: RetrivalRequest, limit: int | None) -> RetrivalJob:
        query = self.query
        if limit:
            query = f"SELECT TOP {limit} sub.* FROM ({query}) sub"

        return SqlServerJob(self.config, query=query, requests=[request])

    def features_for(self, facts: RetrivalJob, request: RetrivalRequest) -> RetrivalJob:
        raise NotImplementedError(f"Not implemented for {type(self)}")


@dataclass
class SqlServerConfig(Codable):
    env_var: str
    schema: str | None = None

    @property
    def url(self) -> str:
        import os

        return os.environ[self.env_var]

    @property
    def connection(self) -> Any:  # noqa: ANN401
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        return create_engine(
            URL.create("mssql+pyodbc", query={"odbc_connect": self.url}),
            fast_executemany=True,
        )

    @staticmethod
    def from_url(url: str) -> SqlServerConfig:
        import os

        if "MSSQL_DATABASE" not in os.environ:
            os.environ["MSSQL_DATABASE"] = url
        return SqlServerConfig(env_var="MSSQL_DATABASE")

    @staticmethod
    def localhost(
        db: str,
        credentials: tuple[str, str] | None = None,
    ) -> SqlServerConfig:
        if credentials:
            return SqlServerConfig.from_url(
                f"mssql://{credentials[0]}:{credentials[1]}@127.0.0.1:1433/{db}",
            )
        return SqlServerConfig.from_url(f"mssql://127.0.0.1:1433/{db}")

    def table(
        self,
        table: str,
        mapping_keys: dict[str, str] | None = None,
    ) -> SqlServerDataSource:
        return SqlServerDataSource(
            config=self,
            table=table,
            mapping_keys=mapping_keys or {},
        )

    def fetch(self, query: str) -> SqlServerJob:
        return SqlServerJob(self, query)

    def with_schema(self, schema: str) -> SqlServerConfig:
        return SqlServerConfig(self.env_var, schema)


mssql_aggregations = {
    "concat_string_agg": lambda agg: f"STRING_AGG({agg.key}, '{agg.separator}')",
}


def build_aggregation_over(
    over: AggregateOver,
    features: list[AggregatedFeature],
    source: SqlServerDataSource,
) -> str:
    from aligned.schemas.transformation import PsqlTransformation

    where_clause = ""

    where_condition = over.condition
    if where_condition:
        if not isinstance(where_condition, PsqlTransformation):
            raise ValueError(
                "Unable to do aggregations with where clause that can not be represented as a SQL",
            )
        where_clause = f"WHERE {where_condition.as_psql()}"

    entities = over.group_by_names
    entity_select = ", ".join(entities)

    aggregations: list[str] = []

    for agg in features:
        agg_transformation = agg.derived_feature.transformation
        if agg_transformation.name not in mssql_aggregations:
            raise ValueError(
                "Unable to do aggregations with where clause that can not be represented as a SQL",
            )

        agg_sql = mssql_aggregations[agg_transformation.name](agg_transformation)
        aggregations.append(f"{agg_sql} AS {agg.name}")

    aggregation_select = ", ".join(aggregations)

    table_schema = ""
    if source.config.schema:
        table_schema = f"{source.config.schema}."

    return f"""
SELECT {entity_select}, {aggregation_select}
FROM {table_schema}{source.table}
{where_clause}
GROUP BY {entity_select}
"""


def build_full_select_query_mssql(
    source: SqlServerDataSource,
    request: RetrivalRequest,
    limit: int | None,
) -> str:
    """
    Generates the SQL query needed to select all features related to a psql data source
    """
    all_features = request.feature_names + list(request.entity_names)

    if request.event_timestamp:
        all_features.append(request.event_timestamp.name)

    sql_columns = source.feature_identifier_for(all_features)
    columns = [
        f'"{sql_col}" AS {alias}' if sql_col != alias else sql_col
        for sql_col, alias in zip(sql_columns, all_features, strict=False)
    ]
    column_select = ", ".join(columns)

    config = source.config
    schema = f"{config.schema}." if config.schema else ""

    limit_query = ""
    if limit:
        limit_query = f"TOP {int(limit)}"

    return f'SELECT {limit_query} {column_select} FROM {schema}"{source.table}"'


def insert_mssql_queries(
    data: pd.DataFrame,
    table: str,
    colnames: list[str],
    chunk_size: int = 1000,
) -> list[str]:
    """Generates an insert into query for your needs"""

    if data.empty:
        return []

    queries = []

    if data.shape[0] > chunk_size:
        # Split into chunks (because sql server cant handle bigger batches)
        number_of_chunks = np.ceil(data.shape[0] / chunk_size)
        data_splits = np.array_split(data, number_of_chunks)
    else:
        data_splits = [data]

    for partition in data_splits:
        assert isinstance(partition, pd.DataFrame)
        str_data = partition[colnames].to_csv(
            header=False,
            index=False,
            quotechar="'",
            quoting=2,
            lineterminator="),\n(",
            date_format="%Y-%m-%d %H:%M:%S",
        )
        str_data_good = "(" + str_data[:-3] + ";"

        query = """
        INSERT INTO {} ({}) VALUES
        {}
        """.format(
            table,
            ", ".join(colnames),
            str_data_good,
        )

        queries.append(query)

    return queries


def factual_mssql_query(
    requests: list[RetrivalRequest],
    entities: SqlServerJob,
    sources: dict[str, SqlServerDataSource],
) -> str:
    entity_name = next(iter(requests[0].entity_names))

    entity_sql = (
        f"WITH entities AS (SELECT *, ROW_NUMBER() OVER (ORDER BY {entity_name})"
        f"AS row_id FROM ({entities.query}) entities)"
    )

    full_cte = entity_sql

    all_returned_columns = set()
    cte_joins = []

    for request in requests:
        all_returned_columns.update(request.entity_names)
        all_returned_columns.update(request.feature_names)

        needed_entities = list(request.entity_names)
        source = sources[request.name]

        reverese_map = {}
        if isinstance(source, ColumnFeatureMappable):
            reverese_map = {value: key for key, value in source.mapping_keys.items()}

        columns = ", ".join(request.feature_names)

        table_alias = "feat"
        source_table = source.table
        if source.config.schema:
            source_table = f"{source.config.schema}.{source.table}"

        joins = [f"{table_alias}.{entity} = entities.{entity}" for entity in needed_entities]
        sort = "row_id"

        if request.event_timestamp_request:
            ent_req = request.event_timestamp_request
            ent_col = reverese_map.get(
                ent_req.event_timestamp.name,
                ent_req.event_timestamp.name,
            )
            columns += f", {ent_col}"
            sort = f"{ent_col} DESC"

            if ent_req.entity_column:
                joins.append(
                    f"{table_alias}.{ent_col} <= entities.{ent_req.entity_column}",
                )
                all_returned_columns.add(ent_req.entity_column)

        join_sql = " AND ".join(joins)
        sub_select = f"""SELECT entities.*, {columns}
        FROM entities
        LEFT JOIN {source_table} {table_alias} ON {join_sql}"""

        sub_select = f"""SELECT row_id, {columns}
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY {sort}) as sub_id
    FROM ({sub_select}) {table_alias}
) {table_alias} WHERE sub_id = 1"""

        cte_sql = f"{request.name} AS ({sub_select})"

        full_cte = f"{full_cte}, {cte_sql}"
        cte_joins.append(f"{request.name} ON {request.name}.row_id = entities.row_id")

    returned_columns_sql = ", ".join(all_returned_columns)
    cte_join_sql = "\nLEFT JOIN ".join(cte_joins)
    final_select = f"SELECT {returned_columns_sql} FROM entities\nLEFT JOIN {cte_join_sql}"

    return f"{full_cte} {final_select}"


def df_to_values_query(df: pd.DataFrame, name: str) -> str:
    str_data = df.to_csv(
        header=False,
        index=False,
        quotechar="'",
        quoting=2,
        lineterminator="),\n(",
        date_format="%Y-%m-%d %H:%M:%S",
    )
    str_data_good = "(" + str_data[:-3]
    sql_col_names = ", ".join(df.columns)
    return f"VALUES {str_data_good}) AS {name} ({sql_col_names})"


def upsert_mssql_queries(
    data: pd.DataFrame,
    table: str,
    column_names: list[str],
    on_match_columns: list[str],
    chunk_size: int = 1000,
) -> list[str]:
    if data.empty:
        return []

    queries = []

    if data.shape[0] > chunk_size:
        # Split into chunks (because sql server cant handle bigger batches)
        number_of_chunks = np.ceil(data.shape[0] / chunk_size)
        data_splits = np.array_split(data, number_of_chunks)
    else:
        data_splits = [data]

    for partition in data_splits:
        assert isinstance(partition, pd.DataFrame)
        str_data = partition[column_names].to_csv(
            header=False,
            index=False,
            quotechar="'",
            quoting=2,
            lineterminator="),\n(",
            date_format="%Y-%m-%d %H:%M:%S",
        )
        str_data_good = "(" + str_data[:-3]
        sql_col_names = ", ".join(column_names)
        join_constraints = "  AND ".join(
            [f"{table}.{match_column} = new.{match_column}" for match_column in on_match_columns],
        )
        update_set = ", ".join(
            [
                f"{table}.{column_name} = new.{column_name}"
                for column_name in column_names
                if column_name not in on_match_columns
            ],
        )

        query = f"""
MERGE INTO {table}
USING (VALUES {str_data_good}) AS new ({sql_col_names})
ON {join_constraints}
WHEN MATCHED THEN
    UPDATE SET {update_set}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({sql_col_names})
    VALUES ({sql_col_names});
"""
        queries.append(query)

    return queries


@dataclass
class SqlServerDataSource(
    CodableBatchDataSource,
    ColumnFeatureMappable,
    WritableFeatureSource,
):
    config: SqlServerConfig
    table: str
    mapping_keys: dict[str, str]

    type_name = "sqlserver"

    @property
    def to_markdown(self) -> str:
        return f"""Type: **Sql Server**

Schema: `{self.config.schema}`

Table: `{self.table}`

Connection Env Var: `{self.config.env_var}`

Column Mappings: *{self.mapping_keys}*"""

    def job_group_key(self) -> str:
        return self.config.env_var

    def contains_config(self, config: Any) -> bool:  # noqa: ANN401
        return isinstance(config, SqlServerConfig) and config.env_var == self.config.env_var

    def __hash__(self) -> int:
        return hash(self.table)

    def all_data(self, request: RetrivalRequest, limit: int | None) -> RetrivalJob:
        # if request.aggregated_features:
        #
        # return SqlServerJob(
        if not request.aggregated_features:
            return SqlServerJob(
                config=self.config,
                query=build_full_select_query_mssql(self, request, limit),
                requests=[request],
            )
        else:
            group_bys = request.aggregate_over().items()
            if len(group_bys) > 1:
                raise ValueError(
                    f"Currently support one aggregation when using fetch all, got {len(group_bys)}",
                )

            agg_over, aggregations = next(iter(group_bys))
            agg_sql = build_aggregation_over(agg_over, list(aggregations), self)

            # Removes the "core features" from the aggregation response
            request.features = set()

            # Assumes that we will not derive features based on aggregations
            request.derived_features = set()

            return SqlServerJob(config=self.config, query=agg_sql, requests=[request])

    def all_between_dates(
        self,
        request: RetrivalRequest,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrivalJob:
        from aligned.psql.jobs import build_date_range_query_psql

        return SqlServerJob(
            config=self.config,
            query=build_date_range_query_psql(self, request, start_date, end_date), # type: ignore
            requests=[request],
        )

    @classmethod
    def multi_source_features_for(
        cls: type[SqlServerDataSource],
        facts: RetrivalJob,
        requests: list[tuple[SqlServerDataSource, RetrivalRequest]],
    ) -> RetrivalJob:
        from aligned.local.job import LiteralRetrivalJob

        sources = {request.name: source for source, request in requests}
        reqs = [request for _, request in requests]
        config = next(iter(sources.values())).config

        if isinstance(facts, LiteralRetrivalJob):
            values_sql = df_to_values_query(facts.df.collect().to_pandas(), "entities")
            facts = SqlServerJob(config, query=f"SELECT * FROM ({values_sql}")

        if not isinstance(facts, SqlServerJob):
            raise ValueError(
                f"Sql Source do currently only support sql server jobs as entities. Got {type(facts)}",
            )

        return SqlServerJob(
            config,
            query=factual_mssql_query(reqs, facts, sources),
            requests=reqs,
        )

    async def freshness(self, event_timestamp: EventTimestamp) -> datetime | None:
        import polars as pl

        table = self.table
        if self.config.schema:
            table = f"{self.config.schema}.{self.table}"

        value = pl.read_database(
            f"SELECT MAX({event_timestamp.name}) as freshness FROM {table}",
            connection=self.config.url,
        )["freshness"].max()

        if value:
            if isinstance(value, datetime):
                return value
            else:
                raise ValueError(f"Unsupported freshness value {value}")
        else:
            return None

    async def upsert(self, job: RetrivalJob, requests: list[RetrivalRequest]) -> None:
        import aioodbc

        if len(requests) != 1:
            raise ValueError(
                f"Only support writing for one request, got {len(requests)}.",
            )

        request = requests[0]

        data = await job.to_pandas()
        pool = await aioodbc.create_pool(dsn=self.config.url)

        table = self.table
        if self.config.schema:
            table = f"{self.config.schema}.{self.table}"

        all_columns = request.all_returned_columns
        renamed_columns = self.feature_identifier_for(all_columns)
        rename_map = dict(zip(all_columns, renamed_columns, strict=False))

        data = data.rename(columns=rename_map)

        queries = upsert_mssql_queries(
            data,
            table,
            renamed_columns,
            [rename_map[col] for col in request.entity_names],
        )

        async def insert_query(query: str) -> None:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(query)
                    except Exception as error:
                        logger.error(f"Failed to run query: {query}, error: {error}")
                        raise

        await asyncio.gather(*[insert_query(query) for query in queries])

    async def insert(self, job: RetrivalJob, requests: list[RetrivalRequest]) -> None:
        import aioodbc

        if len(requests) != 1:
            raise ValueError(
                f"Only support writing for one request, got {len(requests)}.",
            )

        request = requests[0]

        data = await job.to_pandas()
        pool = await aioodbc.create_pool(dsn=self.config.url)

        table = self.table
        if self.config.schema:
            table = f"{self.config.schema}.{self.table}"

        queries = insert_mssql_queries(data, table, request.all_returned_columns)

        async def insert_query(query: str) -> None:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(query)
                    except Exception as error:
                        logger.info(f"Failed to run query: {query}, error: {error}")
                        raise

        await asyncio.gather(*[insert_query(query) for query in queries])
