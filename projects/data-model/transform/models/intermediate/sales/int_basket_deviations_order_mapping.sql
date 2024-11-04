with 

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}
)

, order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

-- TODO: Feels a bit redundant to adding this all the time in int tables
, billing_agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to is null

)

-- ASSUMPTION: Only one basket per customer
-- Get the customers most recent deviation for a menu week
, first_customer_deviation as (

    select 
        menu_week_monday_date
        , billing_agreement_id
        , billing_agreement_basket_id
        , min_by(billing_agreement_basket_deviation_origin_id, deviation_created_at) as billing_agreement_basket_deviation_origin_id
        , min(deviation_created_at) as first_deviation_created_at
    from deviations
    where billing_agreement_basket_deviation_origin_id not in (
        '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B', -- Meal-selector
        '6AD5DF6F-6CDA-4E45-96D0-A169B633247C' -- Pre-selector
        )
    group by 1,2,3

)

, last_mealselector_deviation as (

    select 
        deviations.menu_week_monday_date
        , deviations.billing_agreement_id
        , deviations.billing_agreement_basket_id
        , max_by(deviations.billing_agreement_basket_deviation_origin_id, deviations.deviation_created_at) as billing_agreement_basket_deviation_origin_id
        , max(deviations.deviation_created_at) as last_deviation_created_at
    from deviations
    left join first_customer_deviation
        on deviations.menu_week_monday_date = first_customer_deviation.menu_week_monday_date
        and deviations.billing_agreement_basket_id = first_customer_deviation.billing_agreement_basket_id
        and deviations.billing_agreement_id = first_customer_deviation.billing_agreement_id
    where deviations.billing_agreement_basket_deviation_origin_id in (
        '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B', -- Meal-selector
        '6AD5DF6F-6CDA-4E45-96D0-A169B633247C' -- Pre-selector
        )
    and (
        deviations.deviation_created_at < first_customer_deviation.first_deviation_created_at 
        or first_customer_deviation.first_deviation_created_at is null
    )
    group by 1,2,3

)


-- Get the customers most recent deviation for a menu week
, deviations_filter_active as (

    select 
        menu_week_monday_date
        , billing_agreement_id
        , billing_agreement_basket_id
        , product_variation_id
    from deviations
    where deviations.is_active_deviation = true

)

-- Map deviations to order ids
, deviations_order_mapping as (

    select distinct 
        order_lines.billing_agreement_id
        , deviations_filter_active.billing_agreement_basket_id
        , deviations_filter_active.menu_week_monday_date
        , order_lines.billing_agreement_order_id
        , billing_agreements.company_id
        , coalesce(last_mealselector_deviation.billing_agreement_basket_deviation_origin_id, first_customer_deviation.billing_agreement_basket_deviation_origin_id) as billing_agreement_basket_deviation_origin_id
        , coalesce(last_mealselector_deviation.last_deviation_created_at, first_customer_deviation.first_deviation_created_at) as basket_mapping_created_at
    from deviations_filter_active
    left join first_customer_deviation
        on deviations_filter_active.menu_week_monday_date = first_customer_deviation.menu_week_monday_date
        and deviations_filter_active.billing_agreement_basket_id = first_customer_deviation.billing_agreement_basket_id
        and deviations_filter_active.billing_agreement_id = first_customer_deviation.billing_agreement_id
    left join last_mealselector_deviation
        on deviations_filter_active.menu_week_monday_date = last_mealselector_deviation.menu_week_monday_date
        and deviations_filter_active.billing_agreement_basket_id = last_mealselector_deviation.billing_agreement_basket_id
        and deviations_filter_active.billing_agreement_id = last_mealselector_deviation.billing_agreement_id
    left join order_lines
        on deviations_filter_active.menu_week_monday_date = order_lines.menu_week_monday_date
        and deviations_filter_active.billing_agreement_id = order_lines.billing_agreement_id
        and deviations_filter_active.product_variation_id = order_lines.product_variation_id
    left join billing_agreements
        on order_lines.billing_agreement_id = billing_agreements.billing_agreement_id
    -- Only include deviations which has an order
    where order_lines.billing_agreement_order_id is not null

)

select * from deviations_order_mapping
