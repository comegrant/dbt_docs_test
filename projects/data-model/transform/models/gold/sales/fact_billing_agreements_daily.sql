{{
    config(
        materialized='incremental',
        unique_key='pk_fact_billing_agreements_daily',
        on_schema_change='append_new_columns'
    )
}}

with

dates as (

    select * from {{ref('dim_dates')}}
    where

    {% if flags.FULL_REFRESH %}
        date >= '{{ var('full_load_first_date', '2023-01-01') }}'
    {% else %}
        date >= dateadd(day, -{{ var('incremental_number_of_days', 30) }}, current_date)
    {% endif %}
    
    and date <= getdate()

)

, orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}
)

, loyalty_seasons as (

    select * from {{ ref('dim_loyalty_seasons') }}

)

, customer_journey_segments as (

    select * from {{ ref('int_customer_journey_segments') }}

)

, billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, basket_scheduler as (

    select * from {{ ref('cms__billing_agreement_basket_scheduler') }}

)

, baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}
    where valid_to = '{{ var("future_proof_date") }}'
    and basket_type_id = '{{ var("mealbox_basket_type_id") }}'

)

, paused_deliveries as (
    
    select
    baskets.billing_agreement_id
    , basket_scheduler.menu_week_monday_date
    , basket_scheduler.has_delivery
    from baskets
    left join basket_scheduler
        on baskets.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    where basket_scheduler.has_delivery = false

)

, billing_agreements_not_deleted as (
    select
    pk_dim_billing_agreements
    , billing_agreement_id
    , company_id
    , first_menu_week_monday_date
    , billing_agreement_status_name
    , loyalty_level_number
    , preference_combination_id
    , valid_from
    , valid_to
    from billing_agreements
    where billing_agreement_status_name <> 'deleted'
)

, billing_agreements_with_orders as  (

    select distinct
    menu_week_monday_date
    , billing_agreement_id
    , true as has_order
    from orders

)

--todo:
-- use int_historic_order_weeks_numbered to get the cutoff date
-- use the cutoff date to join with the billing agreement on the corresponding cutoff date
-- we will then have two fks to the billing agreement: one for the date in question and one at the corresponding cutoff date

, tables_joined as (
    
    select 
    md5(
        cast(
            concat(
                dates.date
                , billing_agreements_not_deleted.billing_agreement_id
            ) as string
        )
    ) as pk_fact_billing_agreements_daily
    
    , dates.date
    , billing_agreements_not_deleted.billing_agreement_id
    , billing_agreements_not_deleted.valid_from as valid_from_billing_agreements
    , billing_agreements_not_deleted.company_id
    , billing_agreements_not_deleted.first_menu_week_monday_date

    , case
        when dates.day_of_week = 1 then true
        else false
      end as is_monday

    , case
        when paused_deliveries.has_delivery = false then true
        else false
      end as is_paused

    , case 
        when billing_agreements_not_deleted.billing_agreement_status_name = 'Active' then true
        else false
      end as is_active

    , case 
        when billing_agreements_not_deleted.billing_agreement_status_name = 'Freezed' then true
        else false
      end as is_freezed

    , coalesce(billing_agreements_with_orders.has_order, false) as has_order

    , billing_agreements_not_deleted.loyalty_level_number

    -- Foreign keys
    
    , dates.pk_dim_dates as fk_dim_dates
    , billing_agreements_not_deleted.pk_dim_billing_agreements as fk_dim_billing_agreements
    , md5(billing_agreements_not_deleted.company_id) as fk_dim_companies
    , datediff(
        dates.date
        , billing_agreements_not_deleted.first_menu_week_monday_date
    ) as fk_dim_periods_since_first_menu_week
    , billing_agreements_not_deleted.preference_combination_id as fk_dim_preference_combinations
    , loyalty_seasons.pk_dim_loyalty_seasons as fk_dim_loyalty_seasons
    , md5( cast( customer_journey_segments.sub_segment_id as string) ) as fk_dim_customer_journey_segments

    from dates

    left join billing_agreements_not_deleted
        on dates.date >= billing_agreements_not_deleted.valid_from 
        and dates.date < billing_agreements_not_deleted.valid_to

    left join billing_agreements_with_orders
        on dates.date = billing_agreements_with_orders.menu_week_monday_date 
        and billing_agreements_not_deleted.billing_agreement_id = billing_agreements_with_orders.billing_agreement_id

    left join paused_deliveries
        on dates.date = paused_deliveries.menu_week_monday_date
        and paused_deliveries.billing_agreement_id = billing_agreements_not_deleted.billing_agreement_id

    left join loyalty_seasons 
        on billing_agreements_not_deleted.company_id = loyalty_seasons.company_id
        and dates.date >= loyalty_seasons.loyalty_season_start_date
        and dates.date < loyalty_seasons.loyalty_season_end_date

    left join customer_journey_segments
        on billing_agreements_not_deleted.billing_agreement_id = customer_journey_segments.billing_agreement_id
        and dates.date >= customer_journey_segments.menu_week_monday_date_from
        and dates.date < customer_journey_segments.menu_week_monday_date_to

    where billing_agreements_not_deleted.billing_agreement_id is not null
)

select * from tables_joined