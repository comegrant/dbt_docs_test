import os
import sys

from dotenv import find_dotenv, load_dotenv

sys.path.append(os.path.join(os.getcwd(), ".."))
# Creating .env variables
load_dotenv(find_dotenv())

DBS = {
    "analytics_db_prod": SqlServer.from_env()
    "analytics_db_qa": SqlServer.from_env()
    "analytics_db_replica": SqlServer.from_env()
    "mldb_prod": SqlServer.from_env()
    "postgres_read_db_prod": PostgreSql.from_env()
}

DB = {
    "analytics_db_prod": {
        "dbname": "AnalyticsDB",
        "user": os.getenv("ANALYTICS_PROD_UID"),
        "passw": os.getenv("ANALYTICS_PROD_PWD"),
        "host": "gg-analytics.database.windows.net",
        "port": 1433,
        "driver": "ODBC Driver 18 for SQL Server",
        "connection_string": os.getenv("ANALYTICS_PROD_DB_CONN_STR"),
    },
    "analytics_db_qa": {
        "dbname": "AnalyticsDB",
        "user": os.getenv("ANALYTICS_QA_UID"),
        "passw": os.getenv("ANALYTICS_QA_PWD"),
        "host": "gg-analytics-qa.database.windows.net",
        "port": 1433,
        "driver": "ODBC Driver 18 for SQL Server",
        "connection_string": os.getenv("ANALYTICS_QA_DB_CONN_STR"),
    },
    "analytics_db_replica": {
        "dbname": "AnalyticsDB",
        "user": os.getenv("ANALYTICS_REP_UID"),
        "passw": os.getenv("ANALYTICS_REP_PWD"),
        "host": "gg-analytics-rep.database.windows.net",
        "port": 1433,
        "driver": "ODBC Driver 18 for SQL Server",
        "connection_string": os.getenv("ANALYTICS_REP_DB_CONN_STR"),
    },
    "mldb_prod": {
        "dbname": "GG-ANALYTICS-INT",
        "user": os.getenv("MLDB_UID"),
        "passw": os.getenv("MLDB_PWD"),
        "host": "ml-analytics-int.database.windows.net",
        "port": 1433,
        "driver": "ODBC Driver 18 for SQL Server",
        "connection_string": os.getenv("ML_DB_CONN_STR"),
    },
    "postgres_read_db_prod": {
        "dbname": "staging",
        "user": "{}@gg-analytics-staging",
        "host": "gg-analytics-staging.postgres.database.azure.com",
        "port": 5432,
        "driver": "org.postgresql.Driver",
    },
}
