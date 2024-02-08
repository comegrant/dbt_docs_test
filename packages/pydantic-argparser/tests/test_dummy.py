import pytest
from pydantic import BaseModel
from pydantic_argparser.parser import decode_args, parser_for


class CliInput(BaseModel):
    name: str
    hobbies: list[str]
    age: int = 20


@pytest.mark.asyncio()
async def test_cli_tool() -> None:
    parser = parser_for(CliInput)

    namespace = parser.parse_args(["--name", "John", "--hobbies", "reading", "running"])
    model = decode_args(namespace, CliInput)

    assert model.name == "John"
    assert model.hobbies == ["reading", "running"]
    assert model.age == 20
