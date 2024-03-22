from aligned import PostgreSQLConfig

from data_contracts.blob_storage import AzureBlobConfig
from data_contracts.sql_server import SqlServerConfig

azure_dl_creds = AzureBlobConfig(
    account_name_env="DATALAKE_SERVICE_ACCOUNT_NAME",
    account_id_env="DATALAKE_STORAGE_ACCOUNT_KEY",
    tenent_id_env="AZURE_TENANT_ID",
    client_id_env="DATALAKE_SERVICE_PRINCIPAL_CLIENT_ID",
    client_secret_env="DATALAKE_SERVICE_PRINCIPAL_CLIENT_SECRET",
)

data_science_data_lake = azure_dl_creds.directory("data-science")

folder = data_science_data_lake.directory("test/data")
model_preds = folder.directory("predictions")

local_mssql = SqlServerConfig("LOCAL_SQL", schema="dbo")

adb = SqlServerConfig("ADB_CONNECTION")

adb_ml = adb.with_schema("ml")
adb_ml_output = adb.with_schema("ml_output")

segment_personas_db = PostgreSQLConfig("SEGMENT_PSQL_DB", schema="personas")
