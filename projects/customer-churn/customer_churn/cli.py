import asyncio

from pydantic_argparser import parse_args

from customer_churn.main import RunArgs, run


async def main() -> None:
    """Main entry point. for the CLI"""
    args = parse_args(RunArgs)
    await run(args)


if __name__ == "__main__":
    asyncio.run(main())
