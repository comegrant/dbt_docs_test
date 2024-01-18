import json
import logging
import sqlalchemy as sa
from sqlalchemy.engine import URL

logger = logging.getLogger()


class DB:
    """Holds the connection to the Database"""

    def __init__(self, db_name, local, password=None):
        """Connect to SQL SERVER db"""
        self.db_name = db_name
        self.local = local

        if self.local:
            import config

            conf = config.DB
        else:
            DB_SETTINGS = "/dbfs/FileStore/tables/dwh_settings.json"
            with open(DB_SETTINGS) as filename:
                conf = json.load(filename)

        if self.db_name == "adb":
            conf_db = conf["read_db"]
            driver = "pyodbc"
        elif self.db_name == "mldb":
            conf_db = conf["write_db"]
            driver = "pyodbc"
        elif self.db_name == "postgres_read":
            conf_db = conf["postgres_read_db"]
            driver = "psycopg2"

        if password is None:
            password = conf_db["passw"]

        self.engine = self.create_engine(driver, conf_db, password)

    def create_engine(self, driver, conf_db, password):
        dbname = conf_db["dbname"]
        user = conf_db["user"]
        host = conf_db["host"]
        port = conf_db["port"]

        if driver == "pyodbc":
            return self.create_mssql_engine(dbname, user, host, port, password)

        if driver == "psycopg2":
            return self.create_postgres_engine(dbname, user, host, port, password)

        if driver == "spark":
            return self.create_spark_engine(dbname, user, host, port, password)

        logger.error("Driver type {} not supported!")

    def create_mssql_engine(self, dbname, user, host, port, password):
        conn_str = f"mssql+pyodbc://{user}:{password}@{host}:{port}/{dbname}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = sa.create_engine(conn_str)
        return engine.connect()

    def create_postgres_engine(self, dbname, user, host, port, password):
        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = sa.create_engine(conn_str)
        return engine.connect()

    def create_spark_engine(self, driver, dbname, user, host, port, password):
        self.jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(
            host, port, dbname
        )

        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": driver,
        }

        return None

    def write_to_db(self, queries):
        """
        Writes the prepared dataframes into tables in ML DB

        Args:
            query : Insert into query
        """
        try:
            print(f"Inserting results in {self.db_name}")
            if (queries is None) or (queries == []):
                print("SQL WARNING: Nothing to query")
            else:
                with self.engine.connect() as connection:
                    for query in queries:
                        for batch in query:
                            connection.execute(batch)

        except Exception as e:
            print("Failure writing into db -", str(e))
