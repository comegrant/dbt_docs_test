from datetime import datetime, timezone

import pytest
from aligned.retrival_job import RetrivalJob
from data_contracts.preselector.store import FailedPreselectorOutput, SuccessfulPreselectorOutput
from preselector.data.models.customer import (
    PreselectorFailedResponse,
    PreselectorPreferenceCompliancy,
    PreselectorSuccessfulResponse,
    PreselectorYearWeekResponse,
)
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek


@pytest.mark.asyncio
async def test_successful_output_is_valid_dataframe() -> None:

    response = PreselectorSuccessfulResponse(
        agreement_id=1337,
        correlation_id="...",
        year_weeks=[
            PreselectorYearWeekResponse(
                year=2024,
                week=10,
                variation_ids=["Acl", "B", "c"],
                main_recipe_ids=[1, 2, 3],
                target_cost_of_food_per_recipe=42.0,
                compliancy=PreselectorPreferenceCompliancy.all_compliant,
                ordered_weeks_ago={
                    4: 202409,
                    5: 202409,
                },
                error_vector={
                    "is_chicken_percentage": 0.334,
                    "mean_cooking_time": 0.230,
                }
            ),
            PreselectorYearWeekResponse(
                year=2024,
                week=11,
                variation_ids=["A", "B", "c"],
                main_recipe_ids=[10, 7, 4],
                target_cost_of_food_per_recipe=42.0,
                compliancy=PreselectorPreferenceCompliancy.all_compliant,
                ordered_weeks_ago={
                    4: 202409,
                    5: 202409,
                    1: 202410,
                    2: 202410,
                    3: 202410,
                },
                error_vector={
                    "is_chicken_percentage": 0.334,
                    "mean_cooking_time": 0.230,
                }
            )
        ],
        concept_preference_ids=["A", "B"],
        taste_preferences=[
            NegativePreference(
                preference_id="A",
                is_allergy=False
            )
        ],
        override_deviation=True,
        model_version="version 1",
        generated_at=datetime.now(tz=timezone.utc),
        number_of_recipes=None,
        portion_size=2,
        version=1,
        company_id="09ECD4F0-AE58-4539-8E8F-9275B1859A19",
        has_data_processing_consent=True
    )

    df = response.to_dataframe()

    assert df.height == 2

    expected_request = SuccessfulPreselectorOutput.query().request
    validated_df = await RetrivalJob.from_convertable(
        df, [expected_request]
    ).drop_invalid().select(expected_request.all_returned_columns).to_polars()

    assert df.height == validated_df.height


@pytest.mark.asyncio
async def test_failed_request_to_dataframe() -> None:

    request = PreselectorFailedResponse(
        error_message="Unknown error",
        error_code=500,
        request=GenerateMealkitRequest(
            agreement_id=420,
            company_id="Something",
            compute_for=[
                YearWeek(year=2024, week=20),
                YearWeek(year=2024, week=21),
            ],
            concept_preference_ids=["ID"],
            taste_preferences=[],
            portion_size=4,
            number_of_recipes=3,
            override_deviation=True,
            ordered_weeks_ago={},
            has_data_processing_consent=False
        )
    )

    df = request.to_dataframe()

    assert df.height == 1

    expected_request = FailedPreselectorOutput.query().request
    validated_df = await RetrivalJob.from_convertable(
        df, [expected_request]
    ).drop_invalid().select(expected_request.all_returned_columns).to_polars()

    assert validated_df.height == df.height
