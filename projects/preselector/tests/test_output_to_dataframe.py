from datetime import datetime, timezone

import pytest
from aligned.retrieval_job import RetrievalJob
from data_contracts.preselector.store import FailedPreselectorOutput, Preselector, SuccessfulPreselectorOutput
from preselector.data.models.customer import (
    PreselectorFailedResponse,
    PreselectorPreferenceCompliancy,
    PreselectorRecipeResponse,
    PreselectorSuccessfulResponse,
    PreselectorYearWeekResponse,
)
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.stream import PreselectorResultWriter

response = PreselectorSuccessfulResponse(
    agreement_id=1337,
    correlation_id="...",
    year_weeks=[
        PreselectorYearWeekResponse(
            year=2024,
            week=10,
            target_cost_of_food_per_recipe=42.0,
            ordered_weeks_ago={
                4: 202409,
                5: 202409,
            },
            error_vector={
                "is_chicken_percentage": 0.334,
                "mean_cooking_time": 0.230,
            },
            recipe_data=[
                PreselectorRecipeResponse(
                    main_recipe_id=1, variation_id="Acl", compliancy=PreselectorPreferenceCompliancy.all_compliant
                ),
                PreselectorRecipeResponse(
                    main_recipe_id=2, variation_id="B", compliancy=PreselectorPreferenceCompliancy.all_compliant
                ),
                PreselectorRecipeResponse(
                    main_recipe_id=3, variation_id="c", compliancy=PreselectorPreferenceCompliancy.all_compliant
                ),
            ],
        ),
        PreselectorYearWeekResponse(
            year=2024,
            week=11,
            target_cost_of_food_per_recipe=42.0,
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
            },
            recipe_data=[
                PreselectorRecipeResponse(
                    main_recipe_id=10, variation_id="A", compliancy=PreselectorPreferenceCompliancy.all_compliant
                ),
                PreselectorRecipeResponse(
                    main_recipe_id=7, variation_id="B", compliancy=PreselectorPreferenceCompliancy.all_compliant
                ),
                PreselectorRecipeResponse(
                    main_recipe_id=4, variation_id="c", compliancy=PreselectorPreferenceCompliancy.all_compliant
                ),
            ],
        ),
    ],
    concept_preference_ids=["A", "B"],
    taste_preferences=[NegativePreference(preference_id="A", is_allergy=False)],
    override_deviation=True,
    model_version="version 1",
    generated_at=datetime.now(tz=timezone.utc),
    number_of_recipes=None,
    portion_size=2,
    version=1,
    company_id="09ECD4F0-AE58-4539-8E8F-9275B1859A19",
    has_data_processing_consent=True,
)


def test_migrate_to_new_schema() -> None:
    import json

    model = response.year_weeks[0].model_dump()
    del model["recipe_data"]
    res = PreselectorYearWeekResponse.model_validate_json(json.dumps(model))
    assert len(res.recipe_data) == len(res.variation_ids)


@pytest.mark.asyncio
async def test_successful_output_is_valid_dataframe() -> None:
    df = response.to_dataframe()

    assert df.height == 2

    expected_request = SuccessfulPreselectorOutput.query().request
    validated_df = (
        await RetrievalJob.from_convertable(df, [expected_request])
        .drop_invalid()
        .select(expected_request.all_returned_columns)
        .to_polars()
    )

    assert "recipes" in df.columns
    assert "main_recipe_ids" in df.columns
    assert df.height == validated_df.height


@pytest.mark.asyncio
async def test_batch_output_is_valid_dataframe() -> None:
    from aligned import FileSource

    df = response.to_dataframe()

    assert df.height == 2

    expected_request = Preselector.query().request
    validated_df = df.select(expected_request.all_returned_columns)

    assert "recipes" in df.columns
    assert "main_recipe_ids" in df.columns
    assert df.height == validated_df.height
    assert df["weeks_since_selected"].null_count() == 2
    assert (df["taste_preference_ids"] == ["A"]).all()
    assert df["taste_preferences"].null_count() == 0

    sink = FileSource.parquet_at("data/test.parquet")
    await sink.delete()

    writer = PreselectorResultWriter(company_id="...", sink=sink)

    without_quarantining = response.copy()
    without_quarantining.year_weeks = [week.copy(update={"ordered_weeks_ago": None}) for week in response.year_weeks]
    new_df = without_quarantining.to_dataframe()
    assert new_df["weeks_since_selected"].null_count() == 2

    await writer.batch_write([response, without_quarantining])

    written_df = await sink.to_polars()
    assert written_df.height == 4


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
            has_data_processing_consent=False,
        ),
    )

    df = request.to_dataframe()

    assert df.height == 1

    expected_request = FailedPreselectorOutput.query().request
    validated_df = (
        await RetrievalJob.from_convertable(df, [expected_request])
        .drop_invalid()
        .select(expected_request.all_returned_columns)
        .to_polars()
    )

    assert validated_df.height == df.height
