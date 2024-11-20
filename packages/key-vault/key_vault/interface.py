
from typing import Any, Callable, Protocol, TypeVar

from pydantic_settings import BaseSettings

T = TypeVar('T', bound=BaseSettings)

class KeyVaultInterface(Protocol):

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

        vault = AzureKeyVault.from_vault_name("kv-chefdp-common")
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
        ...
