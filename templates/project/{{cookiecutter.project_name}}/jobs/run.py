import asyncio
import logging
from contextlib import suppress
from typing import Literal

from catalog_connector import connection
from key_vault import key_vault
from key_vault.interface import KeyVaultInterface
from mlflow.models.signature import infer_signature
from model_registry import ModelRegistryBuilder, default_to_databricks_registry, training_run_url
from {{cookiecutter.module_name}}.data import training_dataset
from {{cookiecutter.module_name}}.evaluate_model import evaluate_model
from {{cookiecutter.module_name}}.train import train_model
from pydantic import BaseModel, Field
from pydantic_argparser import parse_args


logger = logging.getLogger(__name__)


class SampleTableArgs(BaseModel):
    table: str
    username: str | None = Field(None)
    environment: Literal["prod", "test", "dev"] = Field("dev")
    model_name: str = Field("mloutputs.example_dummy_model")


async def main(
    args: SampleTableArgs,
    registry: ModelRegistryBuilder | None = None,
    vault: KeyVaultInterface | None = None
) -> None:
    """
    The main function that contains the logic for this job
    """
    if registry is None:
        registry = default_to_databricks_registry(args.username)
    if vault is None:
        vault = key_vault(env=args.environment)

    await vault.load_into_env(connection.env_keys)

    logger.info(args)

    data, feature_refs = await training_dataset(args.table)
    model = train_model(data)
    evaluate_model(model, data)

    await (
        registry.alias("champion")
        .training_dataset(data)
        .training_run_url(run_url())
        .signature(infer_signature(data, model.predict(data)))
        .feature_references(feature_refs)
        .register_as(args.model_name, model)  # type: ignore
    )

def run_url() -> str | None:
    "This is it's own function so we can mock the output in test."
    return training_run_url(connection.spark())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with suppress(ImportError):
        # This is needed in order to run async code in Databricks
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main(
        args=parse_args(SampleTableArgs)
    ))
