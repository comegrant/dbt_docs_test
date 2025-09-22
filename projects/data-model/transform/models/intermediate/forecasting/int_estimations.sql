{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

{% set current_timestamp = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}
{% set number_of_estimated_weeks = 12 %}

with 

dates as (
    
    select * from {{ref('data_platform__dates')}}

)

, agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to = '{{ var("future_proof_date") }}'
)

, baskets as (

    select * from {{ref('cms__billing_agreement_baskets')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

, basket_products as (

    select * from {{ref('int_subscribed_products_scd2')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

, basket_scheduler as (

    select * from {{ref('cms__billing_agreement_basket_scheduler')}}

)

, deviations as (

    select * from {{ref('cms__billing_agreement_basket_deviations')}}

)

, deviation_products as (

    select * from {{ref('cms__billing_agreement_basket_deviation_products')}}

)

, addresses as (

    select * from {{ ref('cms__addresses') }}

)

, timeblock_postal_code_blacklist as (

    select * from {{ ref('int_upcoming_blacklisted_timeblocks_and_postal_codes') }}

)

, last_menu_week as (

    select * from {{ ref('int_menu_weeks') }}
    where is_latest_menu_week_passed_cutoff is true

)

, relevant_period as (

    select  

    company_id
    , year_of_calendar_week as menu_year
    , calendar_week as menu_week
    , date as menu_week_monday_date

    from dates
    left join last_menu_week
    on 1=1
    where 
        date > greatest(last_menu_week.menu_week_monday_date, getdate())
        and date <= dateadd(week, {{number_of_estimated_weeks}}, greatest(last_menu_week.menu_week_monday_date, getdate()) )
        and weekday_name = 'Monday'

)

-- Assuming only one basket of the mealbox type per billing agreement
-- Should add a test for this
, mealbox_baskets as (

    select * from baskets
    where basket_type_id = '{{ var("mealbox_basket_type_id") }}'
)

-- Scheduler should follow the mealbox, not the groceries. 
, mealbox_basket_scheduler as (

    select 
    
    basket_scheduler.* 
    , mealbox_baskets.billing_agreement_id
    
    from basket_scheduler
    inner join mealbox_baskets
        on basket_scheduler.billing_agreement_basket_id = mealbox_baskets.billing_agreement_basket_id
    
)

, baskets_with_company as 
(
    select

          baskets.*
        , agreements.company_id
    
    from baskets
    left join agreements
        on agreements.billing_agreement_id = baskets.billing_agreement_id
    where 
        baskets.is_active_basket = true 
        and agreements.billing_agreement_status_id = 10 --active agreements

)

, basket_and_period_joined as (

    select

          relevant_period.menu_year
        , relevant_period.menu_week
        , relevant_period.menu_week_monday_date
        , baskets_with_company.billing_agreement_id
        , baskets_with_company.billing_agreement_basket_id
        , baskets_with_company.company_id
        , baskets_with_company.basket_type_id
        , mealbox_baskets.basket_delivery_week_type_id
        , mealbox_baskets.shipping_address_id
        , mealbox_baskets.timeblock_id

    from baskets_with_company
    left join relevant_period
        on baskets_with_company.company_id = relevant_period.company_id
    
    -- Assuming only one basket of the mealbox type per billing agreement
    -- Should add a test for this
    left join mealbox_baskets
        on baskets_with_company.billing_agreement_id = mealbox_baskets.billing_agreement_id
)

, basket_even_weeks_default_schedule as (
    -- All baskets with default schedule for even weeks
    select 
    
    basket_and_period_joined.*
    -- get shipping_address_id and/or timeblock_id in the mealbox_basket_scheduler if they exist else use the shipping_address_id and/or timeblock_id in the mealbox_basket
    , coalesce(mealbox_basket_scheduler.shipping_address_id, basket_and_period_joined.shipping_address_id) as scheduled_shipping_address_id
    , coalesce(mealbox_basket_scheduler.timeblock_id, basket_and_period_joined.timeblock_id) as scheduled_timeblock_id
    
    from basket_and_period_joined
    left join mealbox_basket_scheduler
        on basket_and_period_joined.billing_agreement_id = mealbox_basket_scheduler.billing_agreement_id
        and basket_and_period_joined.menu_week_monday_date = mealbox_basket_scheduler.menu_week_monday_date

    where 
        basket_and_period_joined.menu_week % 2 = 0 --even weeks
        and (mealbox_basket_scheduler.billing_agreement_id is null or mealbox_basket_scheduler.has_delivery = true) --default scheduler
        and basket_and_period_joined.basket_delivery_week_type_id in (10, 20) --10 is every week, 20 is even weeks
)

, basket_odd_weeks_default_schedule as (
    -- All baskets with default schedule for odd weeks
    select 
    
    basket_and_period_joined.*
    -- get shipping_address_id and/or timeblock_id in the mealbox_basket_scheduler if they exist else use the shipping_address_id and/or timeblock_id in the mealbox_basket
    , coalesce(mealbox_basket_scheduler.shipping_address_id, basket_and_period_joined.shipping_address_id) as scheduled_shipping_address_id
    , coalesce(mealbox_basket_scheduler.timeblock_id, basket_and_period_joined.timeblock_id) as scheduled_timeblock_id
    
    from basket_and_period_joined
    left join mealbox_basket_scheduler
        on basket_and_period_joined.billing_agreement_id = mealbox_basket_scheduler.billing_agreement_id
        and basket_and_period_joined.menu_week_monday_date = mealbox_basket_scheduler.menu_week_monday_date

    where 
        basket_and_period_joined.menu_week % 2 = 1 --odd weeks
        and (mealbox_basket_scheduler.billing_agreement_id is null or mealbox_basket_scheduler.has_delivery = true) --default scheduler
        and basket_and_period_joined.basket_delivery_week_type_id in (10, 30) --10 is every week, 30 is odd weeks
)

, basket_odd_weeks_scheduled_on_even_weeks as (
    -- All baskets with delivery week type for odd weeks, but that is also scheduled for even weeks

    select 
    
    basket_and_period_joined.*
    -- get shipping_address_id and/or timeblock_id in the mealbox_basket_scheduler if they exist else use the shipping_address_id and/or timeblock_id in the mealbox_basket 
    , coalesce(mealbox_basket_scheduler.shipping_address_id, basket_and_period_joined.shipping_address_id) as scheduled_shipping_address_id
    , coalesce(mealbox_basket_scheduler.timeblock_id, basket_and_period_joined.timeblock_id) as scheduled_timeblock_id
    
    from basket_and_period_joined
    left join mealbox_basket_scheduler
        on basket_and_period_joined.billing_agreement_id = mealbox_basket_scheduler.billing_agreement_id
        and basket_and_period_joined.menu_week_monday_date = mealbox_basket_scheduler.menu_week_monday_date

    where 
        basket_and_period_joined.menu_week % 2 = 0 --even weeks
        and mealbox_basket_scheduler.has_delivery = true --has_delivery even though delivery_week_type is odd
        and basket_and_period_joined.basket_delivery_week_type_id = 30 --30 is odd weeks
)

, basket_even_weeks_scheduled_on_odd_weeks as (
    -- All baskets with delivery week type for even weeks, but that is also scheduled for odd weeks
    
    select 
    
    basket_and_period_joined.*
    -- get shipping_address_id and/or timeblock_id in the mealbox_basket_scheduler if they exist else use the shipping_address_id and/or timeblock_id in the mealbox_basket
    , coalesce(mealbox_basket_scheduler.shipping_address_id, basket_and_period_joined.shipping_address_id) as scheduled_shipping_address_id
    , coalesce(mealbox_basket_scheduler.timeblock_id, basket_and_period_joined.timeblock_id) as scheduled_timeblock_id
    
    from basket_and_period_joined
    left join mealbox_basket_scheduler
        on basket_and_period_joined.billing_agreement_id = mealbox_basket_scheduler.billing_agreement_id
        and basket_and_period_joined.menu_week_monday_date = mealbox_basket_scheduler.menu_week_monday_date

    where 
        basket_and_period_joined.menu_week % 2 = 1 --odd weeks
        and mealbox_basket_scheduler.has_delivery = true --has_delivery even though delivery_week_type is even
        and basket_and_period_joined.basket_delivery_week_type_id = 20 --20 is even weeks
)

, basket_filtered_by_scheduler as (

    select * from basket_even_weeks_default_schedule
    union all 
    select * from basket_odd_weeks_default_schedule
    union all
    select * from basket_odd_weeks_scheduled_on_even_weeks
    union all
    select * from basket_even_weeks_scheduled_on_odd_weeks
    
)

, deviations_filtered_by_basket as (
    
    select

      deviations.*
    , basket_filtered_by_scheduler.billing_agreement_id
    , basket_filtered_by_scheduler.scheduled_shipping_address_id
    , basket_filtered_by_scheduler.scheduled_timeblock_id
    
    from deviations
    left join basket_filtered_by_scheduler
        on basket_filtered_by_scheduler.billing_agreement_basket_id = deviations.billing_agreement_basket_id
        and basket_filtered_by_scheduler.menu_week_monday_date = deviations.menu_week_monday_date
    where 
        basket_filtered_by_scheduler.billing_agreement_basket_id is not null
        and deviations.is_active_deviation = true

)

, baskets_without_deviations as (

    select 

    basket_filtered_by_scheduler .*

    from basket_filtered_by_scheduler 
    left join deviations_filtered_by_basket
        on  deviations_filtered_by_basket.billing_agreement_basket_id = basket_filtered_by_scheduler .billing_agreement_basket_id
        and deviations_filtered_by_basket.menu_week_monday_date = basket_filtered_by_scheduler .menu_week_monday_date
    where 
        deviations_filtered_by_basket.billing_agreement_basket_deviation_id is null 

)

, baskets_with_products as (

    select 

        baskets_without_deviations.menu_year
        , baskets_without_deviations.menu_week
        , baskets_without_deviations.menu_week_monday_date
        , baskets_without_deviations.billing_agreement_basket_id
        , basket_products.product_variation_id
        , basket_products.product_variation_quantity
        , baskets_without_deviations.company_id
        , '00000000-0000-0000-0000-000000000000' as billing_agreement_basket_deviation_origin_id
        , baskets_without_deviations.billing_agreement_id
        , baskets_without_deviations.scheduled_shipping_address_id
        , baskets_without_deviations.scheduled_timeblock_id

    from baskets_without_deviations
    left join basket_products
        on baskets_without_deviations.billing_agreement_basket_id = basket_products.billing_agreement_basket_id

)

, deviations_with_products as (

    select

          deviations_filtered_by_basket.menu_year
        , deviations_filtered_by_basket.menu_week
        , deviations_filtered_by_basket.menu_week_monday_date
        , deviations_filtered_by_basket.billing_agreement_basket_id
        , deviation_products.product_variation_id
        , deviation_products.product_variation_quantity
        , baskets_with_company.company_id
        , deviations_filtered_by_basket.billing_agreement_basket_deviation_origin_id
        , deviations_filtered_by_basket.billing_agreement_id
        , deviations_filtered_by_basket.scheduled_shipping_address_id
        , deviations_filtered_by_basket.scheduled_timeblock_id

    from deviations_filtered_by_basket
    left join deviation_products
        on deviations_filtered_by_basket.billing_agreement_basket_deviation_id = deviation_products.billing_agreement_basket_deviation_id
    left join baskets_with_company
        on baskets_with_company.billing_agreement_basket_id = deviations_filtered_by_basket.billing_agreement_basket_id

)

, basket_and_deviations_unioned as (

    select * from baskets_with_products
    
    union all

    select * from deviations_with_products

)

, with_postal_codes_not_geo_restricted as (

    select 
        basket_and_deviations_unioned.*
        , addresses.postal_code_id

    from basket_and_deviations_unioned
    left join addresses 
        on basket_and_deviations_unioned.scheduled_shipping_address_id = addresses.shipping_address_id
    where 
        addresses.is_geo_restricted = 0 -- when this is 0, the address is able to receive deliveries

)

, baskets_in_timeblocks_not_blacklisted as (

    select 
        with_postal_codes_not_geo_restricted.*

    from with_postal_codes_not_geo_restricted
    left join timeblock_postal_code_blacklist 
        on with_postal_codes_not_geo_restricted.scheduled_timeblock_id = timeblock_postal_code_blacklist.timeblock_id
        and with_postal_codes_not_geo_restricted.menu_year = timeblock_postal_code_blacklist.menu_year
        and with_postal_codes_not_geo_restricted.menu_week = timeblock_postal_code_blacklist.menu_week
        and with_postal_codes_not_geo_restricted.company_id = timeblock_postal_code_blacklist.company_id
        and with_postal_codes_not_geo_restricted.postal_code_id = timeblock_postal_code_blacklist.postal_code_id
    where 
        timeblock_postal_code_blacklist.timeblocks_blacklisted_id is null -- either the scheduled timeblock is not on the blacklist
        or timeblock_postal_code_blacklist.fallback_timeblock_id is not null -- or there is a fallback timeblock

)

, with_timestamp as (
    select

      billing_agreement_id
    , billing_agreement_basket_id
    , company_id
    , menu_year
    , menu_week
    , menu_week_monday_date
    , product_variation_id
    , product_variation_quantity
    , scheduled_shipping_address_id as shipping_address_id
    , postal_code_id
    , billing_agreement_basket_deviation_origin_id
    , '{{current_timestamp}}' as estimation_generated_at

    from baskets_in_timeblocks_not_blacklisted
    
    -- If there are baskets with no subscribed products or deviations
    -- they will be included in the result set with null as product_variation_id
    -- We want to exclude these, as they are not relevant for the estimations
    -- We can not exclude them in the previous steps, as we need the basket id to be able to join with the deviations
    where 
        product_variation_id is not null
)

select * from with_timestamp