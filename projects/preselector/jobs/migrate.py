import asyncio
import logging
from contextlib import suppress
from typing import Literal

from pydantic import BaseModel
from pydantic_argparser import parse_args

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    env: Literal["test", "prod", "dev"]


async def migrate() -> None:
    pass


async def main() -> None:
    import os

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("azure").setLevel(logging.ERROR)

    args = parse_args(RunArgs)

    os.environ["UC_ENV"] = args.env
    os.environ["DATALAKE_ENV"] = args.env

    await migrate()


if __name__ == "__main__":
    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main())
