from contextlib import suppress

from key_vault.azure import AzureKeyVault
from key_vault.databricks import DatabricksKeyVault
from key_vault.interface import KeyVaultInterface


def datalake_vault_keys() -> dict[str, str]:
    return {
        "datalake_service_account_name": "azure-storageAccount-experimental-name",
        "datalake_storage_account_key": "azure-storageAccount-experimental-key",
    }


def databricks_vault_keys(env: str) -> dict[str, str]:
    return {
        "databricks_host": f"databricks-workspace-url-{env}",
        "databricks_token": f"databricks-sp-databricksReader-pat-{env}",
    }


def key_vault(env: str | None = None, global_key_mappings: dict[str, str] | None = None) -> KeyVaultInterface:
    if global_key_mappings is None:
        global_key_mappings = {**datalake_vault_keys(), **databricks_vault_keys(env or "dev")}

    with suppress(ImportError):
        from pyspark.errors.exceptions.base import PySparkRuntimeError  # type: ignore

        with suppress(PySparkRuntimeError, RuntimeError):
            # Should only work on databricks compute
            from pyspark.dbutils import DBUtils  # type: ignore

            return DatabricksKeyVault(
                dbutils=DBUtils(),  # type: ignore
                scope="auth_common",
                global_key_mappings=global_key_mappings,
            )

    return AzureKeyVault.from_vault_name("kv-chefdp-common", global_key_mappings=global_key_mappings)


__all__ = ["AzureKeyVault", "DatabricksKeyVault", "KeyVaultInterface", "key_vault"]
