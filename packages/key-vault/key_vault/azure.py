from __future__ import annotations

import logging
from dataclasses import dataclass
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

    async def load(
        self,
        model: type[T],
        env: str | None = None,
        key_map: dict[str, str] | Callable[[str], str] | None = None,
        custom_values: dict[str, Any] | None = None,
    ) -> T:
        import os

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

            if isinstance(key_map, Callable):
                keys_to_load[name] = key_map(name)
            elif name in key_map:
                keys_to_load[name] = key_map[name]
            else:
                splits = name.split("_")
                if len(splits) != 1:
                    new_name = f"{splits[0]}-{''.join(splits[1:])}"
                else:
                    new_name = splits[0]
                # store secrets as kebab-case
                keys_to_load[name] = new_name

            if env is not None:
                keys_to_load[name] = keys_to_load[name] + f"-{env}"


        for key, value in keys_to_load.items():
            if key in os.environ or key.upper() in os.environ:
                logger.info(f"Found value for {key} in environments, so will not read from key vault.")
                continue

            try:
                logger.info(f"Fetching secret for {key}")
                secret_value = self.client.get_secret(value).value

                if secret_value:
                    os.environ[key] = secret_value
                    all_values[key] = secret_value

            except ResourceNotFoundError as e:
                if key not in can_be_missing:
                    raise ValueError(f"Did not find secret for {key}, tried to load {value}") from e

                logger.info(f"Found no value for {key}. Will use default value.")

        return model(**all_values) # type: ignore

    @staticmethod
    def from_vault_name(vault_name: str, credentials: TokenCredential | None = None) -> AzureKeyVault:
        """
                                        This assumes that you either have logged in through the azure cli,
                                        or that you have set the env vars for Azure authentication.
                                        As it uses the `DefaultAzureCredential` auth method by default.
        """
        return AzureKeyVault(
            SecretClient(
                f"https://{vault_name}.vault.azure.net",
                credential=credentials or DefaultAzureCredential()
            )
        )
