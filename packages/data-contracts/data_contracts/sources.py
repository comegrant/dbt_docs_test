from os import getenv

from aligned import PostgreSQLConfig, RedisConfig

from data_contracts.azure_blob import AzureBlobConfig
from data_contracts.sql_server import SqlServerConfig
from data_contracts.unity_catalog import DatabricksConnectionConfig, UnityCatalog, UnityCatalogSchema

azure_dl_creds = AzureBlobConfig(
    account_name_env="DATALAKE_SERVICE_ACCOUNT_NAME",
    account_id_env="DATALAKE_STORAGE_ACCOUNT_KEY",
    tenant_id_env="AZURE_TENANT_ID",
    client_id_env="DATALAKE_SERVICE_PRINCIPAL_CLIENT_ID",
    client_secret_env="DATALAKE_SERVICE_PRINCIPAL_CLIENT_SECRET",
)

# Azure DL Container
data_science_data_lake = azure_dl_creds.directory("data-science").directory(
    getenv("DATALAKE_ENV", "dev").lower() # I hate this solution, but it will have to do for now
)

# Data Lake Directories
materialized_data = data_science_data_lake.directory("materialized_data")
recommendations_dir = data_science_data_lake.directory("recommendations")


local_mssql = SqlServerConfig("LOCAL_SQL", schema="dbo")

adb = SqlServerConfig("ADB_CONNECTION")
pim_core = SqlServerConfig("CORE_PIM_CONNECTION")

adb_ml = adb.with_schema("ml")
adb_ml_output = adb.with_schema("ml_output")

redis_cluster = RedisConfig("REDIS_URL")

segment_personas_db = PostgreSQLConfig("SEGMENT_PSQL_DB", schema="personas")

def databricks_catalog(catalog: str | None = None) -> UnityCatalog:
    import os

    if catalog is None:
        catalog = os.getenv("UC_ENV", "dev")

    return DatabricksConnectionConfig.on_databricks_only().catalog(catalog)

def ml_features(catalog: str | None = None) -> UnityCatalogSchema:
    return databricks_catalog(catalog).schema("mlfeatures")

def dbt_gold(catalog: str | None = None) -> UnityCatalogSchema:
    return databricks_catalog(catalog).schema("gold")
