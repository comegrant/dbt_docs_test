{{
    config(
        materialized='incremental',
        unique_key='pk_fact_billing_agreements_weekly',
        on_schema_change='append_new_columns'
    )
}}

{% if is_incremental() %}
    {% set days_to_subtract = var('incremental_number_of_days') %}
    {% set today = run_started_at.strftime('%Y-%m-%d') %}
    {% set start_date = dateadd(today,-days_to_subtract,'day') %}
{% else %}
    {% set start_date = var('full_load_first_date') %}
{% endif %}

with

dates as (

    select * from {{ref('dim_dates')}}

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
    where valid_to = '{{ var("future_proof_date") }}'

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
    pk_dim_dates
    , date as monday_date
    , year_of_calendar_week as year
    , calendar_week as week
    from dates
    where 
    weekday_name = 'Monday'
    and date >= '{{start_date}}'
    and date <= getdate()

)

, dates_and_agreements_joined as (
    
    select 
    md5(
        cast(
            concat(
                dates_filtered.monday_date
                , billing_agreements.billing_agreement_id
            ) as string
        )
    ) as pk_fact_billing_agreements_weekly
    , dates_filtered.pk_dim_dates as fk_dim_date
    , billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
    , md5(billing_agreements.company_id) AS fk_dim_companies
    , dates_filtered.monday_date
    , dates_filtered.week
    , dates_filtered.year
    , billing_agreements.billing_agreement_id
    , billing_agreements.valid_from as valid_from_billing_agreements
    , billing_agreements.company_id
    , billing_agreements.first_menu_week_monday_date
    , cast( datediff(dates_filtered.monday_date, billing_agreements.first_menu_week_monday_date) as int) as days_since_first_order
    , coalesce(billing_agreements_with_orders.has_order,0) as has_order
    , case
        when paused_deliveries.has_delivery = false then true
        else false
      end as is_paused
    , case 
        when billing_agreements.billing_agreement_status_name = 'Freezed' then true
        else false
      end as is_freezed
    , case 
        when billing_agreements.billing_agreement_status_name = 'Deleted' then true
        else false
      end as is_deleted
    from dates_filtered
    left join billing_agreements
        on dates_filtered.monday_date >= billing_agreements.valid_from 
        and dates_filtered.monday_date < billing_agreements.valid_to
    left join billing_agreements_with_orders
        on dates_filtered.monday_date = billing_agreements_with_orders.menu_week_monday_date 
        and billing_agreements.billing_agreement_id = billing_agreements_with_orders.billing_agreement_id
    left join paused_deliveries
        on dates_filtered.monday_date = paused_deliveries.menu_week_monday_date
        and paused_deliveries.billing_agreement_id = billing_agreements.billing_agreement_id
    where billing_agreements.billing_agreement_id is not null
)

select * from dates_and_agreements_joined 