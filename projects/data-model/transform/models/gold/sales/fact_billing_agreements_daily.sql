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

, menu_weeks as (

    select * from {{ ref('int_historic_menu_weeks_numbered') }}
    
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
    -- TODO: This is a quick fix as many billing agreements has duplicate schedulers for 2025 week 1,
    -- as they also have a scheduler for 2024 week 53, which is the same week. 
    -- I am following up with cms on how to handle this --Anna
    -- E.g.  menu_week_monday_date = '2024-12-30' and billing_agreement_id = 209774
    , min(basket_scheduler.has_delivery) as has_delivery
    from baskets
    left join basket_scheduler
        on baskets.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    where basket_scheduler.has_delivery = false
    group by all

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
-- find the order placement date for the billing agreements, like we do in fact_orders
-- and use this to get an extra foreign key to dim_billing_agreements.
-- we will then have three fks to the billing agreement: 
--  - one for the date in question 
--  - one at the corresponding cutoff date
--  - one at order placement (i.e. last time the billing agreement changed their order)

, tables_joined as (
    
    select 
    md5(
        cast(
            concat(
                dates.date
                , billing_agreements.billing_agreement_id
            ) as string
        )
    ) as pk_fact_billing_agreements_daily
    
    , dates.date
    , billing_agreements.billing_agreement_id
    , billing_agreements.valid_from as valid_from_billing_agreements
    , billing_agreements_cutoff.valid_from as valid_from_billing_agreements_cutoff
    , billing_agreements.company_id
    , billing_agreements.first_menu_week_monday_date

    , case
        when dates.day_of_week = 1 then true
        else false
      end as is_monday

    , case
        when dates.day_of_week <> 1 then null
        when paused_deliveries.has_delivery = false then true
        else false
      end as is_paused

    , case 
        when billing_agreements.billing_agreement_status_name = 'Active' then true
        else false
      end as is_active

    , case 
        when billing_agreements.billing_agreement_status_name = 'Freezed' then true
        else false
      end as is_freezed

    , case
        when dates.day_of_week <> 1 then null
        else coalesce(billing_agreements_with_orders.has_order, false) 
    end as has_order

    , billing_agreements.loyalty_level_number

    -- Foreign keys
    
    , dates.pk_dim_dates as fk_dim_dates
    , billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
    , billing_agreements_cutoff.pk_dim_billing_agreements as fk_dim_billing_agreements_cutoff
    , md5(billing_agreements.company_id) as fk_dim_companies
    , case
        when dates.date < billing_agreements.first_menu_week_monday_date then null
        else datediff(
            dates.date
            , billing_agreements.first_menu_week_monday_date
        )
    end as fk_dim_periods_since_first_menu_week
    , billing_agreements.preference_combination_id as fk_dim_preference_combinations
    , loyalty_seasons.pk_dim_loyalty_seasons as fk_dim_loyalty_seasons
    , md5( cast( coalesce(customer_journey_segments.sub_segment_id,-1) as string) ) as fk_dim_customer_journey_segments

    from dates

    left join billing_agreements_not_deleted as billing_agreements
        on dates.date >= billing_agreements.valid_from 
        and dates.date < billing_agreements.valid_to

    left join menu_weeks
        on dates.date = menu_weeks.menu_week_monday_date
        and billing_agreements.company_id = menu_weeks.company_id

    left join billing_agreements_not_deleted as billing_agreements_cutoff
        on menu_weeks.menu_week_cutoff_time >= billing_agreements_cutoff.valid_from 
        and menu_weeks.menu_week_cutoff_time < billing_agreements_cutoff.valid_to
        and billing_agreements.billing_agreement_id = billing_agreements_cutoff.billing_agreement_id

    left join billing_agreements_with_orders
        on dates.date = billing_agreements_with_orders.menu_week_monday_date 
        and billing_agreements.billing_agreement_id = billing_agreements_with_orders.billing_agreement_id

    left join paused_deliveries
        on dates.date = paused_deliveries.menu_week_monday_date
        and billing_agreements.billing_agreement_id = paused_deliveries.billing_agreement_id

    left join loyalty_seasons
        on billing_agreements.company_id = loyalty_seasons.company_id
        and dates.date >= loyalty_seasons.loyalty_season_start_date
        and dates.date < loyalty_seasons.loyalty_season_end_date

    left join customer_journey_segments
        on billing_agreements.billing_agreement_id = customer_journey_segments.billing_agreement_id
        and dates.date >= customer_journey_segments.menu_week_monday_date_from
        and dates.date < customer_journey_segments.menu_week_monday_date_to

    where billing_agreements.billing_agreement_id is not null
)

select * from tables_joined