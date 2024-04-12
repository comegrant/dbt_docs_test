"""
Connect to the datalake by setting the spark configuration value
"""
import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def connect_to_datalake_spark(
    application_id: str | None = None,
    authentication_key: str | None = None,
    tenand_id: str | None = None,
) -> None:
    """
    Create spark connection to datalake
    """
    spark = SparkSession.builder.getOrCreate()

    if not application_id:
        application_id = os.getenv("DATALAKE_SERVICE_PRINCIPAL_CLIENT_ID")

    if not tenand_id:
        tenand_id = os.getenv("AZURE_TENANT_ID")

    if not authentication_key:
        authentication_key = os.getenv("DATALAKE_SERVICE_PRINCIPAL_CLIENT_SECRET")

    # Connecting using Service Principal secrets and OAuth
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": ("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"),
        "fs.azure.account.oauth2.client.id": application_id,
        "fs.azure.account.oauth2.client.secret": authentication_key,
        "fs.azure.account.oauth2.client.endpoint": (f"https://login.microsoftonline.com/{tenand_id}/oauth2/token"),
    }

    # Set the azure configs directly in spark conf
    for key, value in configs.items():
        spark.conf.set(key, value)
