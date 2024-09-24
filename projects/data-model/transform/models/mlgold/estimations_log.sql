{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}

{% set current_timestamp = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}
{% set number_of_estimated_weeks = 14 %}

with 

dates as (
    
    select * from {{ref('dim_date')}}

)

, agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to is null
)

, baskets as (

    select * from {{ref('cms__billing_agreement_baskets')}}
    where valid_to is null

)

, basket_products as (

    select * from {{ref('cms__billing_agreement_basket_products')}}
    where valid_to is null

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

, relevant_period as (

    select  

      year as menu_year
    , week as menu_week
    , date as menu_week_monday_date

    from dates
    where date > getdate() and date <= dateadd(week, 14, getdate())
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
    where baskets.is_active = true 
    and agreements.billing_agreement_status_id = 10 --active agreements
    and baskets.is_active = true

)

, basket_and_period_joined as (

    select

          relevant_period.menu_year
        , relevant_period.menu_week
        , relevant_period.menu_week_monday_date
        , baskets_with_company.billing_agreement_id
        , baskets_with_company.billing_agreement_basket_id
        , baskets_with_company.company_id
        , baskets_with_company.delivery_week_type_id

    from baskets_with_company
    left join relevant_period
    on 1=1
)

, basket_even_weeks_default_schedule as (
    -- All baskets with defualt schedule for even weeks
    select 
    
    basket_and_period_joined.*
    
    from basket_and_period_joined
    left join basket_scheduler
    on basket_and_period_joined.billing_agreement_basket_id = basket_scheduler.billing_agreement_basket_id
    and basket_and_period_joined.menu_week_monday_date = basket_scheduler.menu_week_monday_date

    where 
    basket_and_period_joined.menu_week % 2 = 0 --even weeks
    and (basket_scheduler.billing_agreement_basket_id is null or basket_scheduler.has_delivery = true) --default scheduler
    and basket_and_period_joined.delivery_week_type_id in (10, 20) --10 is every week, 20 is even weeks
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
    and basket_and_period_joined.delivery_week_type_id in (10, 30) --10 is every week, 30 is odd weeks
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
    and basket_and_period_joined.delivery_week_type_id = 30 --30 is odd weeks
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
    and basket_and_period_joined.delivery_week_type_id = 20 --20 is even weeks
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
    and deviations.is_active = true

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

, baskets_grouped as(

    select

          menu_year
        , menu_week
        , company_id
        , product_variation_id
        , '00000000-0000-0000-0000-000000000000' as billing_agreement_basket_deviation_origin_id
        , sum(product_variation_quantity) as product_variation_quantity

    from baskets_with_products
    group by 1,2,3,4,5

)

, deviations_grouped as(

    select

          menu_year
        , menu_week
        , company_id
        , product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , sum(product_variation_quantity) as product_variation_quantity

    from deviations_with_products
    group by 1,2,3,4,5

)

, agreements_with_mealbox_adjustments as (

    select

        menu_year
        , menu_week
        , deviations_with_products.company_id
        , '10000000-0000-0000-0000-000000000000' as product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , count(distinct deviations_with_products.billing_agreement_id) as product_variation_quantity

    from deviations_with_products
    left join dim_products
    on deviations_with_products.product_variation_id = dim_products.product_variation_id
    and deviations_with_products.company_id = dim_products.company_id
    where dim_products.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' --Velg&Vrak
    and deviations_with_products.company_id in ('09ECD4F0-AE58-4539-8E8F-9275B1859A19', '8A613C15-35E4-471F-91CC-972F933331D7', '6A2D0B60-84D6-4830-9945-58D518D27AC2', '5E65A955-7B1A-446C-B24F-CFE576BF52D7')

    group by 1,2,3,4,5

)

, basket_and_deviations_and_adjustments_unioned as (

    select * from baskets_grouped
    
    union all

    select * from deviations_grouped

    union all

    select * from agreements_with_mealbox_adjustments

)

, renamed as (

    select
        md5(
            concat(
                  cast(menu_year as string)
                , cast(menu_week as string)
                , cast(company_id as string)
                , cast(product_variation_id as string)
                , cast(billing_agreement_basket_deviation_origin_id as string)
                , '{{current_timestamp}}'
            )
        ) as pk_estimations_log
        , menu_year
        , menu_week
        , company_id
        , product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , product_variation_quantity
        , '{{current_timestamp}}' as estimation_generated_at

    from basket_and_deviations_and_adjustments_unioned 
)


select * from renamed