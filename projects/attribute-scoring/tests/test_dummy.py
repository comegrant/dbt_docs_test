import pytest
import asyncio

@pytest.mark.asyncio
async def test_dummy() -> None:
    await asyncio.sleep(0.1)
    assert True
