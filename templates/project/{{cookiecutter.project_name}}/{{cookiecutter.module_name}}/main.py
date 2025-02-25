import asyncio
import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    name: str


async def main(args: RunArgs) -> None:
    """
    The entrypoint for your code.

    Args:
        args: The arguments needed to run your code.
    """
    logger.info(f"Hello, {args.name}!")


if __name__ == "__main__":
    from pydantic_argparser import parse_args

    logging.basicConfig(level=logging.INFO)
    args = parse_args(RunArgs)
    asyncio.run(main(args))
