# Databricks notebook source
# COMMAND ----------

from collections.abc import Sequence
from typing import TYPE_CHECKING

from data_contracts.reci_pick import LatestRecommendations
from pydantic import BaseModel

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging
import os
from collections import defaultdict
from datetime import date, timedelta

dbutils.widgets.text("company_id", "")
dbutils.widgets.text("number_of_weeks", "12")
dbutils.widgets.text("number_of_weeks_from_now", "5")
dbutils.widgets.text("from_date_iso_format", "")
dbutils.widgets.text("environment", defaultValue="")
dbutils.widgets.text("strategies_size", defaultValue="")
dbutils.widgets.text("batch_write_interval", defaultValue="1000")
dbutils.widgets.text("predict_amount", defaultValue="")
dbutils.widgets.text("write_mode", defaultValue="datalake-only")
dbutils.widgets.text("write_table", defaultValue="mloutputs.preselector_batch")

environment = dbutils.widgets.get("environment")

assert isinstance(environment, str)
assert environment != ""

# Need to set this before importing any contracts due to env vars being accessed
# I know this is is a shit design, but it will do for now
os.environ["DATALAKE_ENV"] = environment
os.environ["UC_ENV"] = environment

# COMMAND ----------
from aligned.config_value import EnvironmentValue
from data_contracts.recipe import AllergyPreferences
from data_contracts.sources import databricks_catalog
from key_vault import key_vault
from preselector.data.models.customer import PreselectorFailedResponse
from preselector.process_stream import load_cache, process_stream_batch
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.sql import sql_folder
from preselector.store import preselector_store
from preselector.stream import CustomReader, CustomWriter, MultipleWriter, PreselectorResultWriter

vault = key_vault(env=environment)

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-name",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-key",
)

write_mode = dbutils.widgets.get("write_mode")
write_table = dbutils.widgets.get("write_table")
company_id = dbutils.widgets.get("company_id")
assert company_id, "Need a company id to run"

predict_amount_raw = dbutils.widgets.get("predict_amount")
strategies_size_raw = dbutils.widgets.get("strategies_size")
batch_write_interval = int(dbutils.widgets.get("batch_write_interval"))

if predict_amount_raw:
    predict_amount = int(predict_amount_raw)
else:
    predict_amount = None

if strategies_size_raw:
    strategies_size = int(strategies_size_raw)
else:
    strategies_size = None

number_of_weeks = int(dbutils.widgets.get("number_of_weeks"))
number_of_weeks_from_now = int(dbutils.widgets.get("number_of_weeks_from_now"))
from_date = dbutils.widgets.get("from_date_iso_format")

if from_date:
    from_date = date.fromisoformat(from_date)
else:
    from_date = date.today() + timedelta(weeks=number_of_weeks_from_now)


logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# COMMAND ----------
# new sql query to replace the ADB query
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek  # noqa


def strategies(requests: list[GenerateMealkitRequest], size: int | None = None) -> list[GenerateMealkitRequest]:
    """
    Selects the top n requests based on the negative preferences.

    This is used to reduce the compute time of when validating new versions of the preselector
    """
    if size is None:
        return requests

    def taste_pref_key(prefs: list[NegativePreference]) -> str:
        return ",".join(sorted([pref.preference_id for pref in prefs]))

    ret_requests: dict[str, list[GenerateMealkitRequest]] = defaultdict(list)

    for req in requests:
        key = taste_pref_key(req.taste_preferences)
        current_reqs = ret_requests[key]

        if len(current_reqs) > size:
            continue

        ret_requests[key].append(req)

    reqs: list[GenerateMealkitRequest] = []
    for req in ret_requests.values():
        reqs.extend(req)

    logger.info(
        f"Started with {len(requests)} requests, filtered it down to {len(reqs)}"
        f" with {len(ret_requests)} different combinations"
    )

    return reqs


async def load_requests(number_of_records: int | None) -> list[GenerateMealkitRequest]:
    from data_contracts.sources import databricks_config

    assert isinstance(from_date, date)

    allergens = await AllergyPreferences.query().all().to_polars()
    allergens_map = {row["preference_id"]: row["allergy_name"] for row in allergens.to_dicts()}

    source = databricks_config.sql_file(sql_folder / "batch_predict.sql", format_values={"company_id": company_id})
    if predict_amount:
        source.query += f" limit {predict_amount}"

    year_week_dates = [from_date + timedelta(weeks=week_dif) for week_dif in range(number_of_weeks)]
    year_weeks = [YearWeek(week=week.isocalendar().week, year=week.isocalendar().year) for week in year_week_dates]

    df = await source.all_columns().to_pandas()
    return strategies(
        [
            GenerateMealkitRequest(
                agreement_id=row["agreement_id"],  # type: ignore
                concept_preference_ids=row["concept_preference_ids"].tolist(),  # type: ignore
                taste_preferences=[
                    NegativePreference(preference_id=taste_id, is_allergy=taste_id in allergens_map)
                    for taste_id in row["taste_preference_ids"].tolist()  # type: ignore
                ],
                portion_size=int(row["portion_size"] or 4),
                number_of_recipes=int(row["number_of_recipes"] or 4),
                compute_for=year_weeks,
                company_id=company_id,
                override_deviation=False,
                has_data_processing_consent=True,
            )
            for _, row in df.iterrows()
        ],
        size=strategies_size,
    )


# COMMAND ----------
async def run() -> None:
    store = preselector_store()

    await vault.load_env_keys(
        [env.env for env in store.needed_configs_for(LatestRecommendations) if isinstance(env, EnvironmentValue)]
    )

    def failed_requests(data: Sequence[BaseModel]) -> None:
        for req in data:
            assert isinstance(req, PreselectorFailedResponse)
            if req.error_code > 100:  # noqa: PLR2004
                raise ValueError(f"The preselector failed with an unknown error: {req}")

    write_schema, write_table_name = write_table.split(".")
    db_source = PreselectorResultWriter(
        company_id, sink=databricks_catalog.schema(write_schema).table(write_table_name)
    )

    if write_mode is None or write_mode == "dl":
        writer = MultipleWriter([PreselectorResultWriter(company_id), db_source])
    elif write_mode == "db":
        writer = db_source
    else:
        writer = PreselectorResultWriter(company_id)

    store = await load_cache(store, company_id=company_id)
    read_stream = CustomReader(method=load_requests)
    await process_stream_batch(
        store,
        stream=read_stream,
        successful_output_stream=writer,
        failed_output_stream=CustomWriter(failed_requests),
        write_batch_interval=batch_write_interval,
    )


# COMMAND ----------
await run()  # type: ignore
