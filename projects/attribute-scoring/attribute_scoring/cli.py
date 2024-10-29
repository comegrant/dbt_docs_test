import asyncio
from pydantic_argparser import parse_args
from attribute_scoring.main import RunArgs, run


async def run_from_cli() -> None:
    """Main entry point. for the CLI """
    args = parse_args(RunArgs)
    await run(args)


if __name__ == "__main__":
    asyncio.run(run_from_cli())
