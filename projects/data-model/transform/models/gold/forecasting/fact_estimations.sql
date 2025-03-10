{{
    config(
        materialized='incremental',
        unique_key='pk_fact_estimations',
        on_schema_change='append_new_columns'
    )
}}


with

latest_estimation_in_fact as (
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
        , max(estimation_generated_at) as latest_estimation_generated_at_timestamp
    from estimations
    group by company_id
)

, billing_agreements as (
    select * from {{ ref('dim_billing_agreements') }}
)

, all_tables_joined as (
    select
        -- PKs
        md5(concat(
            estimation_generated_at
            , estimations.menu_year
            , estimations.menu_week
            , estimations.company_id
            , estimations.billing_agreement_id
            , estimations.product_variation_id
            , billing_agreement_basket_deviation_origin_id
        )) as pk_fact_estimations

        -- IDs
        , estimations.menu_year
        , estimations.menu_week
        , estimations.menu_week_monday_date
        , estimations.company_id
        , estimations.billing_agreement_id
        , estimations.product_variation_id
        , estimations.billing_agreement_basket_deviation_origin_id
        , estimations.estimation_generated_at

        -- FKs
        , cast(date_format(estimation_generated_at, 'yyyyMMdd') as int) as fk_dim_date_estimation_generated
        , cast(date_format(estimation_generated_at, 'HHmm') as string) as fk_dim_time_estimation_generated
        , cast(date_format(estimations.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date_menu_week
        , md5(estimations.company_id) as fk_dim_companies
        , md5(concat(estimations.product_variation_id,estimations.company_id)) as fk_dim_products
        , md5(billing_agreement_basket_deviation_origin_id) as fk_dim_basket_deviation_origins
        , billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , product_variation_quantity
        , case
            when estimation_generated_at = latest_estimation_generated_at_timestamp
            then true
            else false
        end is_latest_estimation

    from estimations
    left join latest_estimation_timestamp
        on latest_estimation_timestamp.company_id = estimations.company_id
    left join billing_agreements
        on billing_agreements.billing_agreement_id = estimations.billing_agreement_id
        and billing_agreements.valid_from <= estimations.estimation_generated_at
        and billing_agreements.valid_to > estimations.estimation_generated_at
)

select * from all_tables_joined
