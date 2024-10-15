{{
    config(
        materialized='incremental',
        unique_key='pk_fact_billing_agreements_daily',
        on_schema_change='append_new_columns'
    )
}}

{% if is_incremental() %}
    {% set days_to_subtract = var('incremental_number_of_days') %}
    {% set today = run_started_at.strftime('%Y-%m-%d') %}
    {% set start_date = dateadd(today,-days_to_subtract,'day') %}
{% else %}
    --{% set start_date = var('full_load_first_date') %}
    {% set start_date = '2024-09-23' %}
{% endif %}

with

dates as (

    select * from {{ref('dim_date')}}

)

, billing_agreements as (

    select * from {{ref('dim_billing_agreements')}}

)

, fact_orders as (

    select * from {{ref('fact_orders')}}

)

, basket_scheduler as (

    select * from {{ref('cms__billing_agreement_basket_scheduler')}}

)

, baskets as (

    select * from {{ref('cms__billing_agreement_baskets')}}
    where valid_to is null

)

, paused_deliveries as (
    
    select
    baskets.billing_agreement_id
    , menu_week_monday_date
    , has_delivery
    from baskets
    left join basket_scheduler
    on baskets.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    where has_delivery = false
)

, billing_agreements_not_deleted as (
    select
    pk_dim_billing_agreements
    , billing_agreement_id
    , company_id
    , first_menu_week_monday_date
    , billing_agreement_status_name
    , valid_from
    , valid_to
    from billing_agreements
    where billing_agreement_status_name <> 'deleted'
)

, billing_agreements_with_orders as  (

    select distinct
    menu_week_monday_date
    , billing_agreement_id
    , 1 as has_order
    from fact_orders
    where menu_week_monday_date >= '{{start_date}}'

)

, dates_filtered as (

    select 
    pk_dim_date
    , date
    , year
    , week
    from dates
    where date >= '{{start_date}}'
    and date <= getdate()

)

, dates_and_agreements_joined as (
    
    select 
    md5(
        cast(
            concat(
                dates_filtered.date
                , billing_agreements_not_deleted.billing_agreement_id
            ) as string
        )
    ) as pk_fact_billing_agreements_daily
    , dates_filtered.pk_dim_date as fk_dim_date
    , billing_agreements_not_deleted.pk_dim_billing_agreements as fk_dim_billing_agreements
    , md5(billing_agreements_not_deleted.company_id) AS fk_dim_companies
    , dates_filtered.date
    , dates_filtered.week
    , dates_filtered.year
    , billing_agreements_not_deleted.billing_agreement_id
    , billing_agreements_not_deleted.valid_from as valid_from_billing_agreements
    , billing_agreements_not_deleted.company_id
    , billing_agreements_not_deleted.first_menu_week_monday_date
    , cast( datediff(dates_filtered.date, billing_agreements_not_deleted.first_menu_week_monday_date) as int) as days_since_first_order
    , coalesce(billing_agreements_with_orders.has_order,0) as has_order
    , case
        when paused_deliveries.has_delivery = false then true
        else false
      end as is_paused
    , case 
        when billing_agreements_not_deleted.billing_agreement_status_name = 'Freezed' then true
        else false
      end as is_freezed
    from dates_filtered
    left join billing_agreements_not_deleted
        on dates_filtered.date >= billing_agreements_not_deleted.valid_from 
        and dates_filtered.date < billing_agreements_not_deleted.valid_to
    left join billing_agreements_with_orders
        on dates_filtered.date = billing_agreements_with_orders.menu_week_monday_date 
        and billing_agreements_not_deleted.billing_agreement_id = billing_agreements_with_orders.billing_agreement_id
    left join paused_deliveries
        on dates_filtered.date = paused_deliveries.menu_week_monday_date
        and paused_deliveries.billing_agreement_id = billing_agreements_not_deleted.billing_agreement_id
    where billing_agreements_not_deleted.billing_agreement_id is not null
)

select * from dates_and_agreements_joined