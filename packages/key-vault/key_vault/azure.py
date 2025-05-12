from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pydantic_core import PydanticUndefined

from key_vault.interface import KeyVaultInterface, T

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


logger = logging.getLogger(__name__)


@dataclass
class AzureKeyVault(KeyVaultInterface):
    client: SecretClient
    global_key_mappings: dict[str, str] = field(default_factory=dict)

    async def load_into_env(
        self, keys: dict[str, str] | Iterable[str], optional_keys: Iterable[str] | None = None
    ) -> dict[str, str]:
        import os

        values: dict[str, str] = {}
        optional_keys = set(optional_keys or [])

        if not isinstance(keys, dict):
            keys = {self.global_key_mappings.get(key.lower(), key): key for key in keys}

        for azure_key, env_key in keys.items():
            if env_key in os.environ or env_key.upper() in os.environ:
                logger.info(f"Found value for {env_key} in environments, so will not read from key vault.")
                continue

            try:
                logger.info(f"Fetching secret for {env_key}")
                secret_value = self.client.get_secret(azure_key).value

                if secret_value:
                    os.environ[env_key] = secret_value
                    values[env_key] = secret_value

            except ResourceNotFoundError as e:
                if env_key not in optional_keys:
                    raise ValueError(f"Did not find secret for {env_key}, tried to load {azure_key}") from e

                logger.info(f"Found no value for {env_key}. Will use default value.")

        return values

    async def load(
        self,
        model: type[T],
        env: str | None = None,
        key_map: dict[str, str] | Callable[[str], str] | None = None,
        custom_values: dict[str, Any] | None = None,
    ) -> T:
        if key_map is None:
            key_map = {}

        can_be_missing: set[str] = set()
        keys_to_load: dict[str, str] = {}

        all_values: dict[str, str] = custom_values.copy() if custom_values else {}

        for name, model_field in model.model_fields.items():
            if name in all_values:
                continue

            if model_field.default != PydanticUndefined or model_field.default_factory is not None:
                can_be_missing.add(name)

            if callable(key_map):
                azure_key = key_map(name)
            elif name in key_map:
                azure_key = key_map[name]
            elif name in self.global_key_mappings:
                azure_key = self.global_key_mappings[name.lower()]
            else:
                splits = name.split("_")
                if len(splits) != 1:
                    new_name = f"{splits[0]}-{''.join(splits[1:])}"
                else:
                    new_name = splits[0]
                # store secrets as kebab-case
                azure_key = new_name

            if env is not None:
                azure_key = azure_key + f"-{env}"

            keys_to_load[azure_key] = name

        loaded_values = await self.load_into_env(keys_to_load, can_be_missing)
        for key, value in loaded_values.items():
            all_values[key] = value

        return model(**all_values)  # type: ignore

    @staticmethod
    def from_vault_name(
        vault_name: str, credentials: TokenCredential | None = None, global_key_mappings: dict[str, str] | None = None
    ) -> AzureKeyVault:
        """
        This assumes that you either have logged in through the azure cli,
        or that you have set the env vars for Azure authentication.
        As it uses the `DefaultAzureCredential` auth method by default.
        """
        return AzureKeyVault(
            SecretClient(f"https://{vault_name}.vault.azure.net", credential=credentials or DefaultAzureCredential()),
            global_key_mappings=global_key_mappings or {},
        )
