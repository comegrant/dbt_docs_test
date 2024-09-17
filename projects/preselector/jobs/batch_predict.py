# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils
    dbutils: RemoteDbUtils = "" # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import json
import logging
import os
from datetime import date, timedelta

dbutils.widgets.text("number_of_weeks", "8")
dbutils.widgets.text("from_date_iso_format", "")
dbutils.widgets.text("should_force_update", "false")
dbutils.widgets.text("environment", defaultValue="")

should_force_update = dbutils.widgets.get("should_force_update").lower() == "true"
environment = dbutils.widgets.get("environment")

assert isinstance(environment, str)
assert environment != ""

# Need to set this before importing any contracts due to env vars being accessed
# I know this is is a shit design, but it will do for now
os.environ["DATALAKE_ENV"] = environment


from data_contracts.sources import adb
from preselector.process_stream import load_cache, process_stream_batch
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store
from preselector.stream import LoggerWriter, PreselectorResultWriter, SqlServerStream

os.environ["ADB_CONNECTION"] = dbutils.secrets.get(
    scope="auth_common",
    key="analyticsDb-connectionString",
).replace("ODBC Driver 17", "ODBC Driver 18")

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-name",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-key",
)

company_id = dbutils.widgets.get("company_id")
assert company_id, "Need a company id to run"


number_of_weeks = int(dbutils.widgets.get("number_of_weeks"))
from_date = dbutils.widgets.get("from_date_iso_format")

if from_date:
    from_date = date.fromisoformat(from_date)
else:
    from_date = date.today() + timedelta(weeks=4)


logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(
    logging.ERROR
)


async def run() -> None:
    store = preselector_store()

    year_week_dates = [
        from_date + timedelta(weeks=week_dif) for week_dif in range(number_of_weeks)
    ]
    year_weeks = [
        YearWeek(week=week.isocalendar().week, year=week.year)
        for week in year_week_dates
    ]

    def init_request(*args, **kwargs) -> GenerateMealkitRequest:  # noqa: ANN002, ANN003
        taste_ids = json.loads(kwargs["taste_preference_ids"]) if kwargs["taste_preference_ids"] else []

        return GenerateMealkitRequest(
            agreement_id=kwargs["agreement_id"],
            concept_preference_ids=json.loads(kwargs["concept_preference_ids"]),
            taste_preferences=[
                NegativePreference(preference_id=taste_id, is_allergy=True)
                for taste_id in taste_ids
            ],
            portion_size=int(kwargs["portion_size"] or 4),
            number_of_recipes=int(kwargs["number_of_recipes"] or 4),
            compute_for=year_weeks,
            company_id=company_id,
            override_deviation=False,
            has_data_processing_consent=True
        )

    read_stream = SqlServerStream(
        payload=init_request,
        config=adb,
        sql=f"""declare @concept_preference_type_id uniqueidentifier = '009cf63e-6e84-446c-9ce4-afdbb6bb9687';
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
        ) as concept_preference_ids
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
        ) as taste_preference_ids
    FROM cms.billing_agreement_preference bap
    JOIN cms.preference pref on pref.preference_id = bap.preference_id
    WHERE pref.preference_type_id = @taste_preference_type_id
    GROUP BY bap.agreement_id
    )

    SELECT DISTINCT ba.agreement_id,
        concept_preference_ids,
        taste_preference_ids,
        tr.variation_meals as number_of_recipes,
        tr.variation_portions as portion_size
    FROM cms.billing_agreement ba
    INNER JOIN personas.agreement_traits tr ON tr.agreement_id = ba.agreement_id
    INNER JOIN concept_preferences cp on cp.agreement_id = ba.agreement_id
    LEFT JOIN taste_preferences tp on tp.agreement_id = ba.agreement_id
    WHERE ba.[status] IN (10,20)
    AND tr.weeks_since_last_delivery <= 12
    AND UPPER(ba.company_id) = '{company_id}'"""
    )

    store = await load_cache(store, company_id=company_id)
    await process_stream_batch(
        store,
        stream=read_stream,
        successful_output_stream=PreselectorResultWriter(company_id),
        failed_output_stream=LoggerWriter(
            level="error", context_title="Preselector failed to generate for"
        ),
        write_batch_interval=1000,
    )


await run()
