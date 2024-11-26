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
    
    select * from {{ref('dim_date')}}

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

    select * from {{ref('int_basket_products_scd2')}}
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

, dim_products as (
    select * from {{ref('dim_products')}}
)

, orders as (

    select * from {{ref('cms__billing_agreement_orders')}}
)

, last_menu_week as (

    select 
    agreements.company_id
    , max(menu_week_monday_date) as menu_week_monday_date
    from orders
    left join agreements
        on orders.billing_agreement_id = agreements.billing_agreement_id
    where 
        order_type_id = '5F34860B-7E61-46A0-80F7-98DCDC53BA9E' -- Recurring
        and order_status_id in (
            '4508130E-6BA1-4C14-94A4-A56B074BB135' --Finished
            , '38A5CC25-A639-4433-A8E6-43FB35DABFD9' --Processing
        )
    group by 1

)

, relevant_period as (

    select  

    company_id
    , year_of_week as menu_year
    , week as menu_week
    , date as menu_week_monday_date

    from dates
    left join last_menu_week
    on 1=1
    where 
        date > greatest(last_menu_week.menu_week_monday_date, getdate())
        and date <= dateadd(week, {{number_of_estimated_weeks}}, greatest(last_menu_week.menu_week_monday_date, getdate()) )
        and weekday_name = 'Monday'

)

, baskets_with_company as 
(
    select

          baskets.*
        , agreements.company_id
    
    from baskets
    left join agreements
    on agreements.billing_agreement_id = baskets.billing_agreement_id
    where baskets.is_active_basket = true 
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
        , baskets_with_company.basket_delivery_week_type_id

    from baskets_with_company
    left join relevant_period
    on baskets_with_company.company_id = relevant_period.company_id
)

, basket_even_weeks_default_schedule as (
    -- All baskets with default schedule for even weeks
    select 
    
    basket_and_period_joined.*
    
    from basket_and_period_joined
    left join basket_scheduler
    on basket_and_period_joined.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    and basket_and_period_joined.menu_week_monday_date = basket_scheduler.menu_week_monday_date

    where 
    basket_and_period_joined.menu_week % 2 = 0 --even weeks
    and (basket_scheduler.billing_agreement_basket_id is null or basket_scheduler.has_delivery = true) --default scheduler
    and basket_and_period_joined.basket_delivery_week_type_id in (10, 20) --10 is every week, 20 is even weeks
)

, basket_odd_weeks_default_schedule as (
    -- All baskets with default schedule for odd weeks
    select 
    
    basket_and_period_joined.*
    
    from basket_and_period_joined
    left join basket_scheduler
    on basket_and_period_joined.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    and basket_and_period_joined.menu_week_monday_date = basket_scheduler.menu_week_monday_date

    where 
    basket_and_period_joined.menu_week % 2 = 1 --odd weeks
    and (basket_scheduler.billing_agreement_basket_id is null or basket_scheduler.has_delivery = true) --default scheduler
    and basket_and_period_joined.basket_delivery_week_type_id in (10, 30) --10 is every week, 30 is odd weeks
)

, basket_odd_weeks_scheduled_on_even_weeks as (
    -- All baskets with delivery week type for odd weeks, but that is also scheduled for even weeks

    select 
    
    basket_and_period_joined.*
    
    from basket_and_period_joined
    left join basket_scheduler
    on basket_and_period_joined.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    and basket_and_period_joined.menu_week_monday_date = basket_scheduler.menu_week_monday_date

    where 
    basket_and_period_joined.menu_week % 2 = 0 --even weeks
    and basket_scheduler.has_delivery = true --has_delivery even though delivery_week_type is odd
    and basket_and_period_joined.basket_delivery_week_type_id = 30 --30 is odd weeks
)

, basket_even_weeks_scheduled_on_odd_weeks as (
    -- All baskets with delivery week type for even weeks, but that is also scheduled for odd weeks
    
    select 
    
    basket_and_period_joined.*
    
    from basket_and_period_joined
    left join basket_scheduler
    on basket_and_period_joined.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    and basket_and_period_joined.menu_week_monday_date = basket_scheduler.menu_week_monday_date

    where 
    basket_and_period_joined.menu_week % 2 = 1 --odd weeks
    and basket_scheduler.has_delivery = true --has_delivery even though delivery_week_type is even
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
    
    from deviations
    left join basket_filtered_by_scheduler
    on basket_filtered_by_scheduler.billing_agreement_basket_id = deviations.billing_agreement_basket_id
    and basket_filtered_by_scheduler.menu_week_monday_date = deviations.menu_week_monday_date
    where basket_filtered_by_scheduler.billing_agreement_basket_id is not null
    and deviations.is_active_deviation = true

)

, baskets_without_deviations as (

    select 

    basket_filtered_by_scheduler .*

    from basket_filtered_by_scheduler 
    left join deviations_filtered_by_basket
    on  deviations_filtered_by_basket.billing_agreement_basket_id = basket_filtered_by_scheduler .billing_agreement_basket_id
    and deviations_filtered_by_basket.menu_week_monday_date = basket_filtered_by_scheduler .menu_week_monday_date
    where deviations_filtered_by_basket.billing_agreement_basket_deviation_id is null 

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
    , billing_agreement_basket_deviation_origin_id
    , '{{current_timestamp}}' as estimation_generated_at

    from basket_and_deviations_unioned
)

select * from with_timestamp