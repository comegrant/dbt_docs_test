import asyncio
import logging
import os
from contextlib import suppress
from typing import Literal

from aligned.sources.redis import RedisSource
from data_contracts.config_values import needed_environment_vars
from data_contracts.materialize import materialize_sources_of_type
from data_contracts.recommendations.store import recommendation_feature_contracts
from key_vault import key_vault
from key_vault.interface import KeyVaultInterface
from pydantic import BaseModel, Field
from pydantic_argparser import parse_args

logger = logging.getLogger(__name__)


class MaterializeArgs(BaseModel):
    environment: Literal["dev", "test", "prod"] = Field("dev")
    should_force_update: bool = Field(False)


async def main(args: MaterializeArgs, vault: KeyVaultInterface | None = None) -> None:
    """
    The main function that contains the logic for this job
    """

    env_keys = ["UC_ENV", "DATALAKE_ENV"]

    if vault is None:
        vault = key_vault(env=args.environment)

    for env_key in env_keys:
        if env_key not in os.environ:
            os.environ[env_key] = args.environment

    store = recommendation_feature_contracts()

    needed_envs = needed_environment_vars(store)
    await vault.load_env_keys(needed_envs)

    updated_views = await materialize_sources_of_type(RedisSource, store, should_force_update=args.should_force_update)
    logger.info(updated_views)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)

    with suppress(ImportError):
        # This is needed in order to run async code in Databricks
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main(args=parse_args(MaterializeArgs)))
