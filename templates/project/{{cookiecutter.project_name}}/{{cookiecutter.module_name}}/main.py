from pydantic import BaseModel


class RunArgs(BaseModel):
    name: str


async def run(args: RunArgs) -> None:
    """
    The entrypoint for your code.

    Args:
        args: The arguments needed to run your code.
    """
    print(f"Hello, {args.name}!")
