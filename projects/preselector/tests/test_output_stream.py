import os
from datetime import datetime

import pytest
from aligned.sources.in_mem_source import InMemorySource
from aligned.sources.redis import RedisStreamSource
from data_contracts.preselector.store import Preselector
from preselector.data.models.customer import (
    PreselectorPreferenceCompliancy,
    PreselectorSuccessfulResponse,
    PreselectorYearWeekResponse,
)
from preselector.schemas.batch_request import NegativePreference
from preselector.stream import PreselectorResultStreamWriter, PreselectorResultWriter
from redis import StrictRedis


@pytest.mark.asyncio
@pytest.mark.skipif("REDIS_URL" not in os.environ, reason="Need to set the REDIS_URL env to run")
async def test_write_to_redis() -> None:
    from data_contracts.preselector.store import Preselector

    stream = Preselector.query().view.stream_data_source
    assert stream is not None
    assert isinstance(stream, RedisStreamSource)

    con: StrictRedis = stream.config.redis() # type: ignore
    await con.xtrim(stream.topic_name, maxlen=0)

    latest_timestamp = "0-0"
    output = await PreselectorResultStreamWriter.for_company(company_id="test")
    consumer = stream.consumer(latest_timestamp)

    example_output = PreselectorSuccessfulResponse(
        agreement_id=1,
        correlation_id="abc",
        year_weeks=[
            PreselectorYearWeekResponse(
                year=2024,
                week=50,
                portion_size=4,
                variation_ids=["abc", "bca"],
                main_recipe_ids=[1, 2],
                compliancy=PreselectorPreferenceCompliancy.all_complient,
                target_cost_of_food_per_recipe=39
            )
        ],
        number_of_recipes=4,
        concept_preference_ids=["abc"],
        taste_preferences=[
            NegativePreference(
                preference_id="abc", is_allergy=True
            )
        ],
        override_deviation=True,
        model_version="test",
        generated_at=datetime.now() # noqa: DTZ005
    )

    write_records = [example_output, example_output]
    await output.batch_write(write_records)

    read_records = await consumer.read()
    assert len(read_records) == len(write_records)


@pytest.mark.asyncio
async def test_write_to_preselector_batch_output() -> None:

    source = InMemorySource.empty()
    store = Preselector.query().store.update_source_for(
        Preselector.location, source
    )

    write = PreselectorResultWriter("test", store=store)
    example_output = PreselectorSuccessfulResponse(
        agreement_id=1,
        correlation_id="abc",
        year_weeks=[
            PreselectorYearWeekResponse(
                year=2024,
                week=50,
                portion_size=4,
                variation_ids=["abc", "bca"],
                main_recipe_ids=[1, 2],
                compliancy=PreselectorPreferenceCompliancy.all_complient,
                target_cost_of_food_per_recipe=39
            ),
            PreselectorYearWeekResponse(
                year=2024,
                week=51,
                portion_size=4,
                variation_ids=["abc", "bca"],
                main_recipe_ids=[1, 2],
                compliancy=PreselectorPreferenceCompliancy.all_complient,
                target_cost_of_food_per_recipe=39
            )
        ],
        number_of_recipes=4,
        concept_preference_ids=["abc"],
        taste_preferences=[
            NegativePreference(
                preference_id="abc", is_allergy=True
            )
        ],
        override_deviation=True,
        model_version="test",
        generated_at=datetime.now() # noqa: DTZ005
    )
    await write.batch_write([example_output])

    assert source.data.height == 2, f"Expected two row in the source, got {source.data.height}"
