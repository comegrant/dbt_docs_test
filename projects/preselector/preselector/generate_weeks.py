import asyncio
import json
import logging

from data_contracts.mealkits import DefaultMealboxRecipes
from data_contracts.preselector.store import (
    RecipePreferences,
)
from data_contracts.recommendations.store import recommendation_feature_contracts
from pydantic import SecretStr
from pydantic_settings import BaseSettings

from preselector.process_stream import load_cache, process_stream_batch
from preselector.recipe_contracts import Preselector
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek
from preselector.stream import LoggerWriter, PreselectorResultWriter, SqlServerStream


class GenerateDatasetSettings(BaseSettings):
    company_id: str
    adb_connection: SecretStr
    datalake_service_account_name: str
    datalake_storage_account_key: SecretStr


async def process(settings: GenerateDatasetSettings) -> None:
    from data_contracts.sources import adb

    logging.basicConfig(level=logging.INFO)

    year_weeks = [
        YearWeek(week=29, year=2024),
        YearWeek(week=30, year=2024),
        YearWeek(week=31, year=2024),
        YearWeek(week=32, year=2024),
        YearWeek(week=33, year=2024),
        YearWeek(week=34, year=2024),
        YearWeek(week=35, year=2024),
    ]

    def init_request(*args, **kwargs) -> GenerateMealkitRequest:  # noqa: ANN002, ANN003
        return GenerateMealkitRequest(
            agreement_id=kwargs["agreement_id"],
            concept_preference_ids=json.loads(kwargs["concept_preference_ids"]),
            taste_preference_ids=json.loads(kwargs["taste_preference_ids"])
            if kwargs["taste_preference_ids"]
            else [],
            portion_size=kwargs["portion_size"],
            number_of_recipes=kwargs["number_of_recipes"],
            compute_for=year_weeks,
            company_id=settings.company_id,
            override_deviation=False,
            has_data_processing_consent=True
        )

    read_stream = SqlServerStream(
        payload=init_request,
        config=adb,
        sql="""declare @concept_preference_type_id uniqueidentifier = '009cf63e-6e84-446c-9ce4-afdbb6bb9687';
declare @taste_preference_type_id uniqueidentifier = '4c679266-7dc0-4a8e-b72d-e9bb8dadc7eb';

WITH concept_preferences AS (
    SELECT
        bap.agreement_id,
        CONCAT(
            CONCAT(
                '["',
                STRING_AGG(
                    convert(
                        nvarchar(36),
                        bap.preference_id
                    ),
                    '", "'
                )
            ),
            '"]'
        ) as preference_ids
    FROM cms.billing_agreement_preference bap
    JOIN cms.preference pref on pref.preference_id = bap.preference_id
    WHERE pref.preference_type_id = @concept_preference_type_id
    GROUP BY bap.agreement_id
),

taste_preferences AS (
    SELECT
        bap.agreement_id,
        CONCAT(
            CONCAT(
                '["',
                STRING_AGG(
                    convert(
                        nvarchar(36),
                        bap.preference_id
                    ),
                    '", "'
                )
            ),
            '"]'
        ) as preference_ids
    FROM cms.billing_agreement_preference bap
    JOIN cms.preference pref on pref.preference_id = bap.preference_id
    WHERE pref.preference_type_id = @taste_preference_type_id
    GROUP BY bap.agreement_id
)

SELECT
    at.agreement_id,
    at.variation_meals as number_of_recipes,
    at.variation_portions as portion_size,
    cp.preference_ids as concept_preference_ids,
    tp.preference_ids as taste_preference_ids
FROM personas.agreement_traits at
INNER JOIN concept_preferences cp on cp.agreement_id = at.agreement_id
LEFT JOIN taste_preferences tp on tp.agreement_id = at.agreement_id
WHERE company='Godtlevert'""",
    )

    store = recommendation_feature_contracts()
    store.add_model(Preselector)
    store.add_feature_view(DefaultMealboxRecipes)
    store.add_feature_view(RecipePreferences)

    store = await load_cache(store, company_id=settings.company_id)

    await process_stream_batch(
        store=store,
        stream=read_stream,
        successful_output_stream=PreselectorResultWriter(settings.company_id),
        failed_output_stream=LoggerWriter(),
        write_batch_interval=3000,
    )


if __name__ == "__main__":
    asyncio.run(process(GenerateDatasetSettings())) # type: ignore
