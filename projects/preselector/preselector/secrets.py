import logging
import os
from dataclasses import dataclass

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger(__name__)


@dataclass
class AzureKeyVault:

    secrets_url: str
    key_mappings: dict[str, str]

    async def load_secrets_to_env(self) -> None:
        client = SecretClient(self.secrets_url, credential=DefaultAzureCredential())

        for key, value in self.key_mappings.items():
            secret_value = client.get_secret(key).value
            if secret_value is not None:
                os.environ[value] = secret_value
            else:
                logger.info(f"Found no value for secret '{key}'")
