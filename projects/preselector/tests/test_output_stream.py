import os
from datetime import datetime

import pytest
from aligned.schemas.feature import StaticFeatureTags
from aligned.sources.in_mem_source import InMemorySource
from aligned.sources.redis import RedisStreamSource
from data_contracts.preselector.basket_features import BasketFeatures
from data_contracts.preselector.store import Preselector
from preselector.data.models.customer import (
    PreselectorPreferenceCompliancy,
    PreselectorRecipeResponse,
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
        company_id="Test",
        has_data_processing_consent=True,
        agreement_id=1,
        correlation_id="abc",
        year_weeks=[
            PreselectorYearWeekResponse(
                year=2024,
                week=50,
                target_cost_of_food_per_recipe=39,
                recipes_data=[
                    PreselectorRecipeResponse(
                        main_recipe_id=1,
                        variation_id="abc",
                        compliancy=PreselectorPreferenceCompliancy.all_compliant
                    ),
                    PreselectorRecipeResponse(
                        main_recipe_id=2,
                        variation_id="bca",
                        compliancy=PreselectorPreferenceCompliancy.all_compliant
                    )
                ]
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
        portion_size=4,
        generated_at=datetime.now() # noqa: DTZ005
    )

    write_records = [example_output, example_output]
    await output.batch_write(write_records)

    read_records = await consumer.read()
    assert len(read_records) == len(write_records)


@pytest.mark.asyncio
async def test_write_to_preselector_batch_output() -> None:
    request = Preselector.query().request
    source = InMemorySource.from_values({
        feat: []
        for feat in request.all_returned_columns
    })

    store = Preselector.query().store.update_source_for(
        Preselector.location, source
    )

    features = [
        feat.name
        for feat
        in BasketFeatures.query().request.all_returned_features
        if StaticFeatureTags.is_entity not in (feat.tags or [])
    ]

    write = PreselectorResultWriter("test", store=store)
    example_output = PreselectorSuccessfulResponse(
        company_id="Test",
        has_data_processing_consent=True,
        agreement_id=1,
        correlation_id="abc",
        portion_size=4,
        year_weeks=[
            PreselectorYearWeekResponse(
                year=2024,
                week=50,
                target_cost_of_food_per_recipe=39,
                error_vector={
                    feat: 0.83
                    for feat in features[:10]
                },
                recipes_data=[
                    PreselectorRecipeResponse(
                        main_recipe_id=1,
                        variation_id="abc",
                        compliancy=PreselectorPreferenceCompliancy.all_compliant
                    ),
                    PreselectorRecipeResponse(
                        main_recipe_id=2,
                        variation_id="bca",
                        compliancy=PreselectorPreferenceCompliancy.all_compliant
                    )
                ]
            ),
            PreselectorYearWeekResponse(
                year=2024,
                week=51,
                target_cost_of_food_per_recipe=39,
                error_vector={
                    feat: 0.83
                    for feat in features
                },
                ordered_weeks_ago={
                    1: 202453,
                    2: 202442,
                },
                recipes_data=[
                    PreselectorRecipeResponse(
                        main_recipe_id=1,
                        variation_id="abc",
                        compliancy=PreselectorPreferenceCompliancy.all_compliant
                    ),
                    PreselectorRecipeResponse(
                        main_recipe_id=2,
                        variation_id="bca",
                        compliancy=PreselectorPreferenceCompliancy.all_compliant
                    )
                ]
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
    assert not source.data["error_vector"].has_nulls()
