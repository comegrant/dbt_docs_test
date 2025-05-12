from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from catalog_connector.value import EnvironmentValue, LiteralValue, ValueRepresentable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.session import SparkSession


@dataclass(init=False)
class DatabricksConnectionConfig:
    workspace: ValueRepresentable
    cluster_id: ValueRepresentable | None
    token: ValueRepresentable | None

    def __init__(
        self,
        workspace: str | ValueRepresentable | None,
        cluster_id: str | ValueRepresentable | None,
        token: str | ValueRepresentable | None,
    ) -> None:
        self.workspace = (
            ValueRepresentable.from_value(workspace)
            if workspace
            else EnvironmentValue("DATABRICKS_HOST", default_value="https://adb-4291784437205825.5.azuredatabricks.net")
        )
        self.cluster_id = ValueRepresentable.from_value(cluster_id) if isinstance(cluster_id, str) else cluster_id
        self.token = ValueRepresentable.from_value(token) if token else EnvironmentValue("DATABRICKS_TOKEN")

    def with_auth(
        self, token: str | ValueRepresentable, workspace: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        """
        Creates a new config for the same configuration, but modifies the token
        """
        return DatabricksConnectionConfig(
            cluster_id=self.cluster_id, token=token, workspace=workspace or self.workspace
        )

    @staticmethod
    def session_or_serverless(
        workspace: str | ValueRepresentable | None = None, token: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        """
        Creates a connection config that either uses the created spark session if running in Databricks.

        Or if running on any other compute will we start a new serverless session.

        Args:
            workspace (str | ValueRepresentable | None): The workspace to connect to,
                defaults to the `DATABRICKS_HOST` env var
            token (str | ValueRepresentable | None): The token to authenticate with,
                defaults to the `DATABRICKS_TOKEN` env

        Returns:
            DatabricksConnectionConfig: A config that is ready to connect to a Spark Session
        """
        return DatabricksConnectionConfig(
            cluster_id=None,
            token=token,
            workspace=workspace,
        )

    @staticmethod
    def serverless(
        workspace: str | ValueRepresentable | None = None, token: str | ValueRepresentable | None = None
    ) -> DatabricksConnectionConfig:
        """
        Creates a serverless connection, no matter where you run your code from.

        Args:
            workspace (str | ValueRepresentable | None): The workspace to connect to,
                defaults to the `DATABRICKS_HOST` env var
            token (str | ValueRepresentable | None): The token to authenticate with,
                defaults to the `DATABRICKS_TOKEN` env

        Returns:
            DatabricksConnectionConfig: A config that is ready to connect to a Spark Session

        """
        return DatabricksConnectionConfig(
            cluster_id="serverless",
            token=token,
            workspace=workspace,
        )

    @staticmethod
    def with_cluster_id(
        cluster_id: str | ValueRepresentable,
        workspace: str | ValueRepresentable | None = None,
        token: str | ValueRepresentable | None = None,
    ) -> DatabricksConnectionConfig:
        return DatabricksConnectionConfig(cluster_id=cluster_id, token=token, workspace=workspace)

    @property
    def env_keys(self) -> list[str]:
        potential_vars = [self.workspace, self.cluster_id, self.token]
        return [env_var.env for env_var in potential_vars if isinstance(env_var, EnvironmentValue)]

    def sql(self, query: str) -> DataFrame:
        """
        Returns a spark dataframe that will load the provided SQL query.

        ```python
        from catalog_connector import session_or_serverless

        df = session_or_serverless.sql("SELECT * FROM dev.mloutputs.preselector_batch WHERE ...")
        df.show()
        ```

        Args:
            query (str): The SQL query to load

        Returns:
            DataFrame: A spark dataframe containing the sql output
        """
        spark = self.spark()
        return spark.sql(query)

    def table(self, table: str) -> TableConfig:
        """
        Returns a table that you can read and write to

        Args:
            table (str): The table to read and write to
        """
        return TableConfig(table, self)

    def append_to(self, table: str, dataframe: DataFrame) -> None:
        """
        Appends a dataframe to a given table.

        ```python
        from catalog_connector import session_or_serverless

        df: spark.DataFrame = ...

        session_or_serverless.append_to("mloutputs.some_table", df)
        ```

        Args:
            table (str): The table to append to
            dataframe (DataFrame): The dataframe to append
        """
        dataframe.write.mode("append").saveAsTable(table)

    def overwrite(self, table: str, dataframe: DataFrame) -> None:
        """
        Overwrites a table with a new dataframe

        ```python
        from catalog_connector import session_or_serverless

        df: spark.DataFrame = ...

        session_or_serverless.overwrite("mloutputs.some_table", df)
        ```

        Args:
            table (str): The table to overwrite
            dataframe (DataFrame): The dataframe to overwrite with
        """
        dataframe.write.mode("overwrite").saveAsTable(table)

    def upsert_on(self, columns: list[str], table: str, dataframe: DataFrame) -> None:
        """
        Upserts a dataframe into a table given a set of columns to match on.

        ```python
        from catalog_connector import session_or_serverless

        df: spark.DataFrame = ...

        session_or_serverless.upsert_on(
            columns=["recipe_id", "portion_id"],
            table="mloutputs.some_table",
            dataframe=df
        )
        ```

        Args:
            columns (list[str]): The columns to base the upsert on
            table (str): The table to upsert into
            dataframe (DataFrame): The dataframe to upsert
        """
        conn = self.spark()

        if not conn.catalog.tableExists(table):
            self.append_to(table, dataframe)
        else:
            on_statement = " AND ".join([f"target.{ent} = source.{ent}" for ent in columns])

            temp_table = f"new_values_{str(uuid4()).lower()}"
            dataframe.createOrReplaceTempView(temp_table)
            conn.sql(f"""MERGE INTO {table} AS target
USING {temp_table} AS source
ON {on_statement}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *""")

    def spark(self) -> SparkSession:
        "Creates a spark session"
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

        builder = DatabricksSession.builder.host(self.workspace.read())

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


@dataclass
class TableConfig:
    identifier: str
    config: DatabricksConnectionConfig

    def read(self) -> DataFrame:
        """
        Returns a dataframe containing ready to load the provided table.

        ```python
        from catalog_connector import connection

        df = connection.table("mloutputs.preselector_batch").read()
        df.show()
        ```

        Returns:
            DataFrame: A spark dataframe containing the output
        """
        spark = self.config.spark()
        return spark.table(self.identifier)

    def append(self, dataframe: DataFrame) -> None:
        self.config.append_to(self.identifier, dataframe)

    def upsert_on(self, columns: list[str], dataframe: DataFrame) -> None:
        self.config.upsert_on(columns, self.identifier, dataframe)

    def overwrite(self, dataframe: DataFrame) -> None:
        self.config.overwrite(self.identifier, dataframe)
