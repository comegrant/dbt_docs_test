from pydantic import BaseModel


class RunArgs(BaseModel):
    name: str


async def run(args: RunArgs) -> None:
    print(f"Hello, {args.name}!")
