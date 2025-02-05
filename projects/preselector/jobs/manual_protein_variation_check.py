# Databricks notebook source
from collections.abc import Sequence
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()


# COMMAND ----------
from slack_connector.databricks_job_url import get_databricks_job_url

url_to_job = get_databricks_job_url()

# COMMAND ----------
import json
import logging
import os
from datetime import date, timedelta

from preselector.data.models.customer import PreselectorFailedResponse, PreselectorSuccessfulResponse
from slack_connector.slack_notification import send_slack_notification

dbutils.widgets.text("company_id", "")
dbutils.widgets.text("number_of_weeks", "4")
dbutils.widgets.text("environment", defaultValue="test")
dbutils.widgets.text("batch_write_interval", defaultValue="1000")


environment = dbutils.widgets.get("environment")

logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(logging.ERROR)


assert isinstance(environment, str)
assert environment != ""

# Need to set this before importing any contracts due to env vars being accessed
# I know this is is a shit design, but it will do for now
os.environ["DATALAKE_ENV"] = environment
os.environ["UC_ENV"] = environment
os.environ["DD_DOGSTATSD_DISABLE"] = "True"

# COMMAND ----------
from preselector.process_stream import load_cache, process_stream_batch
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store
from preselector.stream import (
    CustomReader,
    CustomWriter,
    InMemoryWriterReader,
)
from preselector.variationchecks import check_protein_variation

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

from_date = str(date.today())
if from_date:
    from_date = date.fromisoformat(from_date)
else:
    from_date = date.today() + timedelta(weeks=6)


# COMMAND
agreement_id_list = []


async def load_requests(number_of_records: int | None) -> list[GenerateMealkitRequest]:
    from data_contracts.sources import databricks_config

    query_for_batch_predict = f"""
    with active_freeze_billing_agreements as (
        select
            pk_dim_billing_agreements,
            billing_agreement_id,
            company_id,
            is_current
        from gold.dim_billing_agreements
        where billing_agreement_status_name in ('Active', 'Freezed')
            and company_id = '{company_id}'
    ),

    billing_agreement_basket_products as (
        select
            fk_dim_billing_agreements,
            fk_dim_products,
            product_variation_id,
            product_variation_quantity
        from gold.bridge_billing_agreements_basket_products
    ),

    dim_products as (
        select
            *
        from
            gold.dim_products
    ),

    last_delivery_monday as (
        select
            billing_agreement_id,
            max(menu_week_monday_date) as last_delivery_monday
        from gold.fact_orders
        where order_status_id = '4508130E-6BA1-4C14-94A4-A56B074BB135' -- finished
        group by billing_agreement_id
    ),

    days_since_last_delivery as (
        select
            billing_agreement_id,
            datediff(current_date(), last_delivery_monday) as days_since_last_delivery
        from last_delivery_monday
    ),

    preferences as (
    select
        pk_dim_preference_combinations,
        fk_dim_billing_agreements,
        preference_id_combinations_concept_type as concept_preference_ids,
        preference_id_combinations_taste_type as taste_preference_ids
    from gold.dim_preference_combinations
    ),

    valid_portions as (
        select distinct
            portion_quantity
        from gold.fact_menus
        where
            is_locked_recipe
            and menu_week_monday_date > current_date()
            and is_dish
            and has_recipe_portions
            and company_id = '{company_id}'

    ),

    final as (
    select
        active_freeze_billing_agreements.billing_agreement_id as agreement_id,
        case
            when concept_preference_ids is not null then
                concat('["',
                    concat_ws('", "',
                                split(concept_preference_ids, ',')),
                    '"]')
            else
                null
        end as concept_preference_ids,
        case
            when taste_preference_ids is not null then
                concat('["',
                    concat_ws('", "',
                                split(taste_preference_ids, ',')),
                    '"]')
            else
                null
        end as taste_preference_ids,
        dim_products.meals as number_of_recipes,
        dim_products.portions as portion_size
    from
        active_freeze_billing_agreements
    left join
        preferences
        on active_freeze_billing_agreements.pk_dim_billing_agreements
            = preferences.fk_dim_billing_agreements
    left join
        billing_agreement_basket_products
        on active_freeze_billing_agreements.pk_dim_billing_agreements
            = billing_agreement_basket_products.fk_dim_billing_agreements
    left join
        dim_products
        on billing_agreement_basket_products.fk_dim_products = dim_products.pk_dim_products
    inner join
        days_since_last_delivery
        on active_freeze_billing_agreements.billing_agreement_id
            = days_since_last_delivery.billing_agreement_id
    where product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
        and days_since_last_delivery <= 12*7 -- 12 weeks
        and is_current = true
        and portions in (
            select portion_quantity from valid_portions
        )
    )

    select * from final where concept_preference_ids is not null order by agreement_id limit 10000;

    """

    spark = databricks_config.connection()
    sdf = spark.sql(query_for_batch_predict)
    df = sdf.toPandas()
    year_week_dates = [from_date + timedelta(weeks=week_dif) for week_dif in range(number_of_weeks)]
    year_weeks = [YearWeek(week=week.isocalendar().week, year=week.isocalendar().year) for week in year_week_dates]

    requests = [
        GenerateMealkitRequest(
            agreement_id=row["agreement_id"],
            concept_preference_ids=json.loads(row["concept_preference_ids"]),
            taste_preferences=[
                NegativePreference(preference_id=taste_id, is_allergy=True)
                for taste_id in (json.loads(row["taste_preference_ids"]) if row["taste_preference_ids"] else [])
            ],
            portion_size=int(row["portion_size"] or 4),
            number_of_recipes=int(row["number_of_recipes"] or 4),
            compute_for=year_weeks,
            company_id=company_id,
            override_deviation=False,
            has_data_processing_consent=True,
        )
        for _, row in df.iterrows()
    ]

    # Append agreement_id to the list
    agreement_id_list.extend(row["agreement_id"] for _, row in df.iterrows())

    return requests


# COMMAND ----------
protein_results = []
duplicate_protein_weeks = []
agreement_id_problems = []


async def run() -> None:
    store = preselector_store()

    def failed_requests(data: Sequence[BaseModel]) -> None:
        for req in data:
            assert isinstance(req, PreselectorFailedResponse)
            if req.error_code > 100:  # noqa: PLR2004
                raise ValueError(f"The preselector failed with an unknown error code: {data}")

    writer_reader = InMemoryWriterReader()
    store = await load_cache(store, company_id=company_id)
    read_stream = CustomReader(method=load_requests)
    await process_stream_batch(
        store,
        stream=read_stream,
        successful_output_stream=writer_reader,
        failed_output_stream=CustomWriter(failed_requests),
        write_batch_interval=batch_write_interval,
    )

    results = await writer_reader.read()

    for result in results:
        decoded_result = PreselectorSuccessfulResponse.model_validate(result)
        for year_week in decoded_result.year_weeks:
            protein_result, duplicate_info = await check_protein_variation(
                store,
                best_recipe_ids=year_week.main_recipe_ids,
                week=year_week.week,
                agreement_id=decoded_result.agreement_id,
            )
            protein_results.append(protein_result)
            duplicate_protein_weeks.append(duplicate_info)
            if duplicate_info:
                agreement_id_problems.append(decoded_result.agreement_id)

    return protein_results, duplicate_protein_weeks


## COMMAND ----------
try:
    output = await run()
except Exception as error:
    raise error
#
# COMMAND ----------
unique_agreement_id_problems = set(agreement_id_problems)
print(unique_agreement_id_problems)  # noqa: T201
#
#
# COMMAND ----------
clean_duplicate_protein_weeks = [item for item in duplicate_protein_weeks if item.strip()]

if len(clean_duplicate_protein_weeks) > 0:
    send_slack_notification(
        environment="local_dev",  # "local_dev",
        header_message=" ⚠️ protein overload 🫤",
        body_message=(
            f"🙋‍♂️ We want you to eat,\n"
            f"We want you to eat proteins🍗,\n"
            f"We want you to eat loads of proteins. 🍗🍖🥩🐟🐷\n"
            f"But we admit, we did not give you the variety you need to meet 😔\n"
            f"The error count is {len(clean_duplicate_protein_weeks)} cases in {company_id}\n"
            f"This constitutes {len(set(agreement_id_problems))/len(agreement_id_list)*100:.2f} percent of {len(agreement_id_list)} agreements\n"  # noqa: E501
            f"More details can be found here {url_to_job}"
        ),
        relevant_people="niladri, science",
        is_error=False,
    )
