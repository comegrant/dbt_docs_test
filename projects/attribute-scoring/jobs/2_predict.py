import asyncio
import logging
from contextlib import suppress
from pydantic_argparser import parse_args

import os
import mlflow
from attribute_scoring.predict.predict import predict_pipeline
from attribute_scoring.common import ArgsPredict

from key_vault import key_vault
from attribute_scoring.common import DatabricksSecrets


async def main(args: ArgsPredict):
    vault = key_vault(args.env)

    secrets = await vault.load(
        model=DatabricksSecrets,
        key_map={
            "databricks_host": f"databricks-workspace-url-{args.env}",
            "databricks_token": f"databricks-sp-bundle-pat-{args.env}",
        },
    )

    os.environ["DATABRICKS_HOST"] = secrets.databricks_host
    os.environ["DATABRICKS_TOKEN"] = secrets.databricks_token.get_secret_value()
    os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

    mlflow.set_tracking_uri("databricks")
    await predict_pipeline(args=args)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
    logger = logging.getLogger(__name__)

    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main(args=parse_args(ArgsPredict)))
