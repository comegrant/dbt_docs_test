import logging

from cheffelo_logging import DataDogConfig, setup_datadog
from key_vault.azure import AzureKeyVault


def setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
    )
    logging.getLogger("azure").setLevel(logging.ERROR)


async def datadog_logger(env: str | None = None) -> None:
    vault = AzureKeyVault.from_vault_name("kv-chefdp-common")

    settings = await vault.load(
        DataDogConfig,
        custom_values=dict(
            datadog_service_name="analytics-new",
            datadog_tags=f"env:{env}",
            datadog_site="datadoghq.eu",
            datadog_source="python",
        ),
    )
    # The root logger, so we log everything to datadog
    logger = logging.getLogger("")
    setup_logger()
    setup_datadog(logger, settings)
