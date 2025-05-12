from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from pydantic_core import PydanticUndefined

from key_vault.interface import KeyVaultInterface, T

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

logger = logging.getLogger(__name__)


@dataclass
class DatabricksKeyVault(KeyVaultInterface):
    dbutils: RemoteDbUtils
    scope: str
    global_key_mappings: dict[str, str]

    async def load_into_env(
        self, keys: dict[str, str] | Iterable[str], optional_keys: Iterable[str] | None = None
    ) -> dict[str, str]:
        import os

        from pyspark.errors.exceptions.captured import IllegalArgumentException  # type: ignore

        values: dict[str, str] = {}
        optional_keys = set(optional_keys or [])

        if not isinstance(keys, dict):
            keys = {self.global_key_mappings.get(key.lower(), key): key for key in keys}

        for db_key, env_key in keys.items():
            if env_key in os.environ or env_key.upper() in os.environ:
                logger.info(f"Found value for {env_key} in environments, so will not read from key vault.")
                continue

            try:
                logger.info(f"Fetching secret for {env_key}")
                secret_value = self.dbutils.secrets.get(self.scope, db_key)

                if secret_value:
                    os.environ[env_key] = secret_value
                    values[env_key] = secret_value

            except IllegalArgumentException as e:
                if env_key not in optional_keys:
                    raise ValueError(f"Did not find secret for {env_key}, tried to load {db_key}") from e

                logger.info(f"Found no value for {env_key}. Will use default value.")

        return values

    async def load(
        self,
        model: type[T],
        env: str | None = None,
        key_map: dict[str, str] | Callable[[str], str] | None = None,
        custom_values: dict[str, Any] | None = None,
    ) -> T:
        """
        Loads a set of settings variables from the Azure key valut.

        ```python
        class DataDogConfig(BaseSettings):
            datadog_api_key: str
            datadog_service_name: str
            datadog_tags: str

            datadog_site: Annotated[str, Field] = "datadoghq.eu"
            datadog_source: Annotated[str, Field] = "python"

        vault = KeyVault.from_vault_name("kv-chefdp-common")
        dd_config = await vault.load(
            DataDogConfig,
            custom_values={
                "datadog_service_name": "preselector",
                "datadog_tags": f"env:test,image-tag:dev-latest",
            }
        )
        print(dd_config)
        ```
        >>> datadog_api_key='*****' datadog_service_name='preselector' datadog_tags='env:test,image-tag:dev-latest'
            datadog_site='datadoghq.eu' datadog_source='python'
        """
        if key_map is None:
            key_map = {}

        can_be_missing: set[str] = set()
        keys_to_load: dict[str, str] = {}

        all_values: dict[str, str] = custom_values.copy() if custom_values else {}

        for name, field in model.model_fields.items():
            if name in all_values:
                continue

            if field.default != PydanticUndefined or field.default_factory is not None:
                can_be_missing.add(name)

            if callable(key_map):
                db_key = key_map(name)
            elif name in key_map:
                db_key = key_map[name]
            else:
                splits = name.split("_")
                if len(splits) != 1:
                    new_name = f"{splits[0]}-{''.join(splits[1:])}"
                else:
                    new_name = splits[0]
                # store secrets as kebab-case
                db_key = new_name

            if env is not None:
                db_key = db_key + f"-{env}"

            keys_to_load[db_key]

        loaded_values = await self.load_into_env(keys_to_load, optional_keys=can_be_missing)
        for key, value in loaded_values.items():
            all_values[key] = value
        return model(**all_values)  # type: ignore

    @staticmethod
    def from_scope(
        scope: str, dbutils: RemoteDbUtils | None = None, global_key_mappings: dict[str, str] | None = None
    ) -> DatabricksKeyVault:
        """
        This assumes that you either have logged in through the azure cli,
        or that you have set the env vars for Azure authentication.
        As it uses the `DefaultAzureCredential` auth method by default.
        """
        if dbutils is not None:
            return DatabricksKeyVault(scope=scope, dbutils=dbutils, global_key_mappings=global_key_mappings or {})

        try:
            from pyspark.dbutils import dbutils  # type: ignore

            # Only works on the databricks runtime
            return DatabricksKeyVault(
                dbutils=dbutils,  # type: ignore
                scope="auth_common",
                global_key_mappings=global_key_mappings or {},
            )
        except ModuleNotFoundError as e:
            raise ValueError("You need to define a dbutils if not running on Databricks") from e
