{{
    config(
        materialized='incremental',
        unique_key='pk_fact_estimations',
        on_schema_change='append_new_columns'
    )
}}


with

products as (
    select * from {{ ref('dim_products') }}
)

, portions as (
    select * from {{ ref('dim_portions') }}
)

, companies as (
    select * from {{ ref('dim_companies') }}
)

, billing_agreement_preferences as (
    select * from {{ ref('int_billing_agreement_preferences_unioned') }}
)

, latest_estimation_in_fact as (
    select
        distinct estimation_generated_at
    from {{ this }}
    where is_latest_estimation = true
)

, estimations as (
    select
    estimations.*
    from {{ ref('int_estimations') }} as estimations
    {% if is_incremental() %}
    inner join latest_estimation_in_fact
        -- We include the last estimation since we need to update the is_latest_estimation flag
        on estimations.estimation_generated_at >= latest_estimation_in_fact.estimation_generated_at
    {% endif %}
)

, latest_estimation_timestamp as (
    select
        company_id
        , max(estimation_generated_at) as latest_estimation_generated_at
    from estimations
    group by company_id
)

, billing_agreements as (
    select * from {{ ref('dim_billing_agreements') }}
)

, add_financial_date as (
    select
        estimations.*
        , {{ get_financial_date_from_monday_date('estimations.menu_week_monday_date') }} as menu_week_financial_date
    from estimations
)

, add_keys as (
    select
        md5(concat(
            estimation_generated_at
            , add_financial_date.menu_year
            , add_financial_date.menu_week
            , add_financial_date.company_id
            , add_financial_date.billing_agreement_id
            , add_financial_date.product_variation_id
            , billing_agreement_basket_deviation_origin_id
        )) as pk_fact_estimations

        , add_financial_date.menu_year
        , add_financial_date.menu_week
        , add_financial_date.menu_week_monday_date
        , add_financial_date.menu_week_financial_date
        , add_financial_date.company_id
        , add_financial_date.billing_agreement_id
        , add_financial_date.product_variation_id
        , add_financial_date.billing_agreement_basket_deviation_origin_id
        , add_financial_date.estimation_generated_at
        , add_financial_date.product_variation_quantity
        , case
            when add_financial_date.estimation_generated_at = latest_estimation_timestamp.latest_estimation_generated_at
            then true
            else false
        end is_latest_estimation

        , cast(date_format(estimation_generated_at, 'yyyyMMdd') as int) as fk_dim_date_estimation_generated
        , cast(date_format(estimation_generated_at, 'HHmm') as string) as fk_dim_time_estimation_generated
        , cast(date_format(add_financial_date.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_date_menu_week
        , md5(add_financial_date.company_id) as fk_dim_companies
        , md5(concat(add_financial_date.product_variation_id,add_financial_date.company_id)) as fk_dim_products
        , md5(billing_agreement_basket_deviation_origin_id) as fk_dim_basket_deviation_origins
        , billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
        , portions.pk_dim_portions as fk_dim_portions


    from add_financial_date
    left join latest_estimation_timestamp
        on latest_estimation_timestamp.company_id = add_financial_date.company_id
    left join billing_agreements
        on billing_agreements.billing_agreement_id = add_financial_date.billing_agreement_id
        and billing_agreements.valid_from <= add_financial_date.estimation_generated_at
        and billing_agreements.valid_to > add_financial_date.estimation_generated_at
    left join products
        on add_financial_date.product_variation_id = products.product_variation_id
        and add_financial_date.company_id = products.company_id
    left join companies
        on add_financial_date.company_id = companies.company_id
    left join portions
        on products.portion_name = portions.portion_name_local
        and companies.language_id = portions.language_id
    left join billing_agreement_preferences
        on billing_agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id
)

select * from add_keys
