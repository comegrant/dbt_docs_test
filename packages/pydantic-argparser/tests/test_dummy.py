from datetime import date

import pytest
from pydantic import BaseModel
from pydantic_argparser.parser import decode_args, parser_for


class CliInput(BaseModel):
    name: str
    some_date: date
    hobbies: list[str] | None
    age: int = 20


@pytest.mark.asyncio()
async def test_cli_tool() -> None:
    expected_output = CliInput(name="John", hobbies=["reading", "running"], some_date=date(year=2025, month=1, day=10))

    assert expected_output.hobbies is not None

    parser = parser_for(CliInput)

    namespace = parser.parse_args(
        [
            "--name",
            expected_output.name,
            "--hobbies",
            expected_output.hobbies[0],
            expected_output.hobbies[1],
            "--some-date",
            expected_output.some_date.isoformat(),
        ],
    )
    model = decode_args(namespace, CliInput)

    assert model.name == expected_output.name
    assert model.hobbies == expected_output.hobbies
    assert model.age == expected_output.age
    assert model.some_date == expected_output.some_date
