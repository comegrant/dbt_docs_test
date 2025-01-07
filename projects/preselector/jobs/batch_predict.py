# Databricks notebook source
# COMMAND ----------

from collections.abc import Sequence
from typing import TYPE_CHECKING

from pydantic import BaseModel

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

dbutils.widgets.text("company_id", "")
dbutils.widgets.text("number_of_weeks", "8")
dbutils.widgets.text("from_date_iso_format", "")
dbutils.widgets.text("environment", defaultValue="")
dbutils.widgets.text("batch_write_interval", defaultValue="1000")
dbutils.widgets.text("write_mode", defaultValue="dl")

environment = dbutils.widgets.get("environment")

assert isinstance(environment, str)
assert environment != ""

# Need to set this before importing any contracts due to env vars being accessed
# I know this is is a shit design, but it will do for now
os.environ["DATALAKE_ENV"] = environment
os.environ["UC_ENV"] = environment

# COMMAND ----------
from data_contracts.sources import databricks_catalog
from preselector.data.models.customer import PreselectorFailedResponse
from preselector.process_stream import load_cache, process_stream_batch
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store
from preselector.stream import CustomReader, CustomWriter, MultipleWriter, PreselectorResultWriter

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

write_mode = dbutils.widgets.get("write_mode")
company_id = dbutils.widgets.get("company_id")
assert company_id, "Need a company id to run"
batch_write_interval = int(dbutils.widgets.get("batch_write_interval"))


number_of_weeks = int(dbutils.widgets.get("number_of_weeks"))
from_date = dbutils.widgets.get("from_date_iso_format")

if from_date:
    from_date = date.fromisoformat(from_date)
else:
    from_date = date.today() + timedelta(weeks=6)


logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(
    logging.ERROR
)

# COMMAND ----------
# new sql query to replace the ADB query
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek  # noqa

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
            portion_size
        from gold.fact_menus
        where
            weekly_menu_status_code_id = 3 -- published
            and menu_week_monday_date > current_date()
            and is_dish
            and has_recipe_portions
            and portion_status_code_id = 1
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
            select portion_size from valid_portions
        )
    )

    select * from final where concept_preference_ids is not null

    """

    spark = databricks_config.connection()
    sdf = spark.sql(query_for_batch_predict)
    df = sdf.toPandas()
    year_week_dates = [
        from_date + timedelta(weeks=week_dif) for week_dif in range(number_of_weeks)
    ]
    year_weeks = [
        YearWeek(week=week.isocalendar().week, year=week.isocalendar().year)
        for week in year_week_dates
    ]

    return [
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
            has_data_processing_consent=True
        )
        for _, row in df.iterrows()
    ]


# COMMAND ----------
async def run() -> None:
    store = preselector_store()

    def failed_requests(data: Sequence[BaseModel]) -> None:
        for req in data:
            assert isinstance(req, PreselectorFailedResponse)
            if req.error_code > 100: # noqa: PLR2004
                raise ValueError(f"The preselector failed with an unknown error code: {data}")

    db_source = PreselectorResultWriter(
        company_id,
        sink=databricks_catalog.schema("mloutputs").table("preselector_batch")
    )

    if write_mode is None or write_mode == "dl":
        writer = MultipleWriter([
            PreselectorResultWriter(company_id),
            db_source
        ])
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

await run()
