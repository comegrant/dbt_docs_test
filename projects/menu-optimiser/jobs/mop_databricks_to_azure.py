import asyncio
from contextlib import suppress
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field
from pydantic_argparser import parse_args
from key_vault import key_vault
from key_vault.interface import KeyVaultInterface

from catalog_connector import connection
from data_contracts.sources import data_science_data_lake


class Args(BaseModel):
    environment: Literal["prod", "test", "dev"] = Field("dev")


def get_carbs_data() -> pd.DataFrame:
    df = connection.sql("""
    select distinct
        dr.recipe_id,
        dr.recipe_name,
        gi.main_group_name as main_group,
        gi.category_group_name as category_group,
        gi.product_group_name as product_group
    from gold.dim_recipes dr
    left join intermediate.int_recipe_ingredients_joined intt
        on dr.recipe_id = intt.recipe_id
    left join gold.dim_portions dp
        on intt.portion_id = dp.portion_id
    left join gold.dim_ingredients gi
        on intt.ingredient_id = gi.ingredient_id
    where
        dr.is_in_recipe_universe
        and dp.portion_name_local in ('4', '1')
        and intt.is_main_carbohydrate
    """).toPandas()
    return df


def get_protein_data() -> pd.DataFrame:
    df = connection.sql("""
    select distinct
        dr.recipe_id,
        dr.recipe_name,
        gi.main_group_name as main_group,
        gi.category_group_name as category_group,
        gi.product_group_name as product_group
    from gold.dim_recipes dr
    left join intermediate.int_recipe_ingredients_joined intt
        on dr.recipe_id = intt.recipe_id
    left join gold.dim_portions dp
        on intt.portion_id = dp.portion_id
    left join gold.dim_ingredients gi
        on intt.ingredient_id = gi.ingredient_id
    where
        dr.is_in_recipe_universe
        and dp.portion_name_local in ('4', '1')
        and intt.is_main_protein
    """).toPandas()
    return df


def get_ingredients_data() -> pd.DataFrame:
    df = connection.sql("""
    select distinct
        dr.recipe_id,
        gi.ingredient_id,
        gi.ingredient_name,
        gi.is_cold_storage
    from gold.dim_recipes dr
    left join intermediate.int_recipe_ingredients_joined intt
        on dr.recipe_id = intt.recipe_id
    left join gold.dim_portions dp
        on intt.portion_id = dp.portion_id
    left join gold.dim_ingredients gi
        on intt.ingredient_id = gi.ingredient_id
    where
        dr.is_in_recipe_universe
        and dp.portion_name_local in ('4', '1')
        and gi.ingredient_id <> 4062 --generic basis
        and gi.main_group_name <> 'Basis'
    """).toPandas()

    return df


def get_customer_data() -> pd.DataFrame:
    df = connection.sql("""
    with

    agreements as (

        select * from gold.dim_billing_agreements
        where is_current = True
            and billing_agreement_status_name = 'Active'

    )

    , preferences as (

        select * from intermediate.int_billing_agreement_preferences_unioned

    )

    , preference_combinations as (

        select * from gold.dim_preference_combinations

    )

    , products as (

        select * from gold.dim_products
        where product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'

    )

    , bridge_agreement_products as (

        select * from gold.bridge_billing_agreements_basket_products
    )

    , billing_agreement_portions as (

        select
            agreements.billing_agreement_id
            , products.portions
        from agreements
        left join bridge_agreement_products
            on agreements.pk_dim_billing_agreements = bridge_agreement_products.fk_dim_billing_agreements
        left join products
            on bridge_agreement_products.fk_dim_products = products.pk_dim_products

    )

    , billing_agreement_preferences as (

        select
            agreements.company_id
            , agreements.billing_agreement_id
            , agreements.billing_agreement_status_name
            , billing_agreement_portions.portions
            , preference_combinations.concept_name_combinations
            , preference_combinations.concept_preference_id_list
            , preference_combinations.taste_name_combinations_including_allergens
            , preference_combinations.taste_preferences_including_allergens_id_list
        from agreements
        left join billing_agreement_portions
            on agreements.billing_agreement_id = billing_agreement_portions.billing_agreement_id
        left join preferences
            on
                agreements.billing_agreement_preferences_updated_id
                = preferences.billing_agreement_preferences_updated_id
        left join preference_combinations
            on
                preferences.preference_combination_id
                = preference_combinations.pk_dim_preference_combinations
        where preference_combinations.taste_preferences_including_allergens_id_list is not null
    )

    , agreement_preferences_with_traits as (
        select
            bap.company_id
        , md5(concat_ws('-', bap.company_id, bap.taste_preferences_including_allergens_id_list, bap.concept_preference_id_list))
            as negative_taste_preference_combo_id
        , lower(bap.taste_name_combinations_including_allergens) as negative_taste_preferences
        , bap.taste_preferences_including_allergens_id_list      as negative_taste_preferences_ids
        , lower(bap.concept_name_combinations)                   as positive_taste_preferences
        , bap.concept_preference_id_list                         as positive_taste_preferences_ids
        , bap.billing_agreement_id
        , bap.portions
        , bat.customer_journey_sub_segment_name
        from billing_agreement_preferences bap
        left join diamond.billing_agreement_traits bat
        on bap.billing_agreement_id = bat.billing_agreement_id
    )

    , agreement_preferences_aggregated as (
        select
            company_id
        , negative_taste_preference_combo_id
        , negative_taste_preferences
        , negative_taste_preferences_ids
        , positive_taste_preferences
        , positive_taste_preferences_ids
        , count(distinct billing_agreement_id) as number_of_users
        , count(distinct case when portions = 1 then billing_agreement_id end) as users_with_1_portions
        , count(distinct case when portions = 2 then billing_agreement_id end) as users_with_2_portions
        , count(distinct case when portions = 3 then billing_agreement_id end) as users_with_3_portions
        , count(distinct case when portions = 4 then billing_agreement_id end) as users_with_4_portions
        , count(distinct case when portions = 5 then billing_agreement_id end) as users_with_5_portions
        , count(distinct case when portions = 6 then billing_agreement_id end) as users_with_6_portions
        , count(distinct case when customer_journey_sub_segment_name = 'Churned' then billing_agreement_id end) as users_churned
        , count(distinct case when customer_journey_sub_segment_name = 'Reactivated' then billing_agreement_id end) as users_reactivated
        , count(distinct case when customer_journey_sub_segment_name = 'Always On' then billing_agreement_id end) as users_always
        , count(distinct case when customer_journey_sub_segment_name = 'Frequent' then billing_agreement_id end) as users_frequent
        , count(distinct case when customer_journey_sub_segment_name = 'Regular' then billing_agreement_id end) as users_regular
        , count(distinct case when customer_journey_sub_segment_name = 'Occasional' then billing_agreement_id end) as users_occasional
        , count(distinct case when customer_journey_sub_segment_name = 'Onboarding' then billing_agreement_id end) as users_onboarding
        , count(distinct case when customer_journey_sub_segment_name = 'Waiting for Onboarding' then billing_agreement_id end) as users_waiting
        , count(distinct case when customer_journey_sub_segment_name = 'Partial Signup' then billing_agreement_id end) as users_partial
        , count(distinct case when customer_journey_sub_segment_name = 'Regret' then billing_agreement_id end) as users_regret
        , count(distinct case when customer_journey_sub_segment_name = 'Unknown' then billing_agreement_id end) as users_unknown
        from agreement_preferences_with_traits
        group by
            company_id
            , negative_taste_preference_combo_id
            , negative_taste_preferences
            , negative_taste_preferences_ids
            , positive_taste_preferences
            , positive_taste_preferences_ids
    )

    select * from agreement_preferences_aggregated;
    """).toPandas()
    return df


async def write_to_data_lake(df: pd.DataFrame, filename: str, env: str):
    """Write dataframe to azure data lake."""
    df_clean = df.copy()
    df_clean.attrs.clear()

    await data_science_data_lake.config.storage.write(
        path=f"data-science/menu_generator/{env}/{filename}",
        content=df_clean.to_parquet(index=False),
    )


async def main(args: Args, vault: KeyVaultInterface | None = None) -> None:
    if vault is None:
        vault = key_vault(env=args.environment)

    await vault.load_env_keys(
        ["DATALAKE_SERVICE_ACCOUNT_NAME", "DATALAKE_STORAGE_ACCOUNT_KEY"]
    )

    df_carbs = get_carbs_data()
    df_protein = get_protein_data()
    df_ingredients = get_ingredients_data()
    df_customer = get_customer_data()

    await write_to_data_lake(
        df=df_carbs,
        filename="recipe_carbohydrates.parquet",
        env=args.environment,
    )

    await write_to_data_lake(
        df=df_protein,
        filename="recipe_proteins.parquet",
        env=args.environment,
    )

    await write_to_data_lake(
        df=df_ingredients,
        filename="recipe_ingredients.parquet",
        env=args.environment,
    )

    await write_to_data_lake(
        df=df_customer,
        filename="customer_info.parquet",
        env=args.environment,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv())
    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main(args=parse_args(Args)))
