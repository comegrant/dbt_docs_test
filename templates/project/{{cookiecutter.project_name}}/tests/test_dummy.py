import asyncio

import pytest
from pytest_mock import MockFixture


@pytest.mark.asyncio
async def test_dummy(mocker: MockFixture) -> None:
    await asyncio.sleep(0.1)
    assert True
