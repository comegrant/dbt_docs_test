import json
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from lmkgroup_ds_utils.db import config

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class DB:
    """Holds the connection to the Database"""

    def __init__(
        self,
        db_name: str,
        local: bool,
        env: Optional[str] = "prod",
        password: Optional[str] = None,
        use_spark: Optional[bool] = False,
        db_settings_path: str = "/dbfs/FileStore/tables/dwh_settings.json",
    ):
        """Connect to SQL SERVER db"""
        self.db_name = db_name
        self.local = local
        self.use_spark = use_spark
        self.env = env

        if not use_spark:
            self.conf_db = self._load_config(db_settings_path)
            self.conn_str = self._load_connection_string(password)

        self._create_engine(db_name, password)

    def _load_config(self, db_settings_path):
        if self.local:
            conf = config.DB
        else:
            with open(db_settings_path) as filename:
                conf = json.load(filename)

        db_conf_key = "{}_{}".format(self.db_name, self.env)
        if db_conf_key not in conf.keys():
            logger.error(
                "db_name %s_%s not defined in configuration keys",
                self.db_name,
                self.env,
            )
            return None

        return conf[db_conf_key]

    def _load_connection_string(self, password: Optional[str] = None):
        if not password:
            password = self.conf_db["passw"]

        if "connection_string" not in self.conf_db.keys():
            return f"DRIVER={self.conf_db['driver']};DATABASE={self.conf_db['dbname']};UID={self.conf_db['user']};SERVER={self.conf_db['host']};PORT={self.conf_db['port']};PWD={password}"

        return self.conf_db["connection_string"]

    def _create_engine(self, db_name, password):
        if self.use_spark:
            self.jdbc_url, self.connection_properties = self._create_spark_engine(
                self.conf_db, password
            )

        elif db_name in ["analytics_db", "ml_db"]:
            self.engine = self._create_mssql_engine(self.conn_str)

        elif db_name == "postgres_db":
            self.engine = self._create_postgres_engine(self.conn_str)

        else:
            logger.error("Db name %s not supported!", db_name)

    def _create_mssql_engine(self, conn_str):
        """Create the engine for mssql databases"""
        logger.info(
            "Connecting to database %s using conn_str %s", self.db_name, conn_str
        )
        return create_engine(
            URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        )

    def _create_postgres_engine(self, conn_str):
        """Create the engine for postgres databases"""
        logger.info(
            "Connecting to database %s using conn_str %s", self.db_name, conn_str
        )
        return create_engine(
            URL.create("postgresql+psycopg2", query={"odbc_connect": conn_str})
        )

    def _create_spark_engine(self, conf_db, password):
        """Create the connection properties for spark"""
        jdbc_url = f"jdbc:sqlserver://{conf_db['host']}:{conf_db['port']};database={conf_db['dbname']}"
        connection_properties = {
            "user": conf_db["user"],
            "password": password,
            "driver": conf_db["driver"],
        }

        return jdbc_url, connection_properties

    def read_data(self, query):
        """
        Reads data from database using the query sent
        """
        logger.info("Running query on %s", self.db_name)
        if (query is None) or (query == []):
            logger.error("SQL WARNING: Nothing to query")
            return None

        return pd.read_sql(query, self.engine.connect())

    def execute_query(self, query):
        """
        Executes a query to the database

        Args:
            query (str): query to execute
        """

        logger.info("Running query on %s", self.db_name)
        if (query is None) or (query == []):
            logger.error("SQL WARNING: Nothing to query")
            return None

        with self.engine.connect() as connection:
            with connection.begin():
                # run statements in a "begin once" block
                connection.execute(query)

    def write_to_db(self, queries):
        """
        Writes the prepared dataframes into tables in ML DB

        Args:
            query : Insert into query
        """
        logger.info("Inserting results in %s", self.db_name)
        if (queries is None) or (queries == []):
            logging.error("SQL WARNING: Nothing to query")
            return None

        with self.engine.connect() as connection:
            for query in queries:
                for batch in query:
                    connection.execute(batch)

    def write_to_table(self, table_name: str, data: list):
        logger.info("Inserting results in %s", self.db_name)
        if (data is None) or (data == []):
            logger.error("SQL WARNING: Nothing to insert")
            return None

        for row_dct in data:
            logger.info("Inserting row %s to %s", row_dct, table_name)

            # Check that row is of type dict
            if not (type(row_dct) is dict):
                logger.error("SQL WARNING: Row to insert is not of type dict!")
                return None

            columns = row_dct.keys()
            with self.engine.connect() as connection:
                query = "INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})".format(
                    table_name=table_name,
                    columns=", ".join(columns),
                    value_placeholders=", ".join(["?"] * len(row_dct)),
                )
                connection.execute(query, list(row_dct.values()))


if __name__ == "__main__":
    db = DB(db_name="adb", local=True, env="qa")
