import asyncio

async def main() -> None:
    print("Hello from {{cookiecutter.library_name}}!")  # noqa: T201

if __name__ == "__main__":
    asyncio.run(main())
