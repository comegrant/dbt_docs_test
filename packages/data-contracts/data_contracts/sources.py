from aligned import PostgreSQLConfig, RedisConfig

from data_contracts.azure import AzureBlobConfig
from data_contracts.sql_server import SqlServerConfig
from data_contracts.unity_catalog import DatabricksConnectionConfig, EnvironmentValue

azure_dl_creds = AzureBlobConfig(  # type: ignore
    account_name=EnvironmentValue("DATALAKE_SERVICE_ACCOUNT_NAME"),
    account_id=EnvironmentValue("DATALAKE_STORAGE_ACCOUNT_KEY"),
    tenant_id=EnvironmentValue("AZURE_TENANT_ID"),
    client_id=EnvironmentValue("DATALAKE_SERVICE_PRINCIPAL_CLIENT_ID"),
    client_secret=EnvironmentValue("DATALAKE_SERVICE_PRINCIPAL_CLIENT_SECRET"),
)

# Azure DL Container
data_science_data_lake = azure_dl_creds.directory("data-science").directory(
    EnvironmentValue("DATALAKE_ENV", default_value="test")  # type: ignore
)

# Data Lake Directories
materialized_data = data_science_data_lake.directory("materialized_data")
recommendations_dir = data_science_data_lake.directory("recommendations")


local_mssql = SqlServerConfig("LOCAL_SQL", schema="dbo")
adb = SqlServerConfig("ADB_CONNECTION")
pim_core = SqlServerConfig("CORE_PIM_CONNECTION")

adb_ml = adb.with_schema("ml")
adb_ml_output = adb.with_schema("ml_output")

redis_cluster = RedisConfig.from_env("REDIS_URL")
segment_personas_db = PostgreSQLConfig("SEGMENT_PSQL_DB", schema="personas")

databricks_config = DatabricksConnectionConfig.databricks_or_serverless()

databricks_catalog = databricks_config.catalog(EnvironmentValue("UC_ENV"))
ml_features = databricks_catalog.schema("mlfeatures")
ml_outputs = databricks_catalog.schema("mloutputs")
ml_gold = databricks_catalog.schema("mlgold")
dbt_gold = databricks_catalog.schema("gold")
