with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}
)

, order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, billing_agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

-- Find the order that corresponds to the active deviation
, deviations_find_related_order as (

    select distinct
        deviations.billing_agreement_basket_id
        , deviations.menu_week_monday_date
        , order_lines.billing_agreement_order_id
        , order_lines.billing_agreement_id
        , deviations.billing_agreement_basket_deviation_origin_id
    from deviations
    left join order_lines
        on deviations.menu_week_monday_date = order_lines.menu_week_monday_date
        and deviations.billing_agreement_id = order_lines.billing_agreement_id
        and deviations.product_variation_id = order_lines.product_variation_id
    -- the active deviation is the one that corresponds to the order product variations
    where deviations.is_active_deviation = true
    -- only include deviations that have an order
    and order_lines.billing_agreement_order_id is not null

)

, deviations_add_aggregated_columns as (

    select
        billing_agreement_basket_id
        , menu_week_monday_date
        , max(is_onesub_migration_insert + is_onesub_migration_update) as is_onesub_migration
        -- get the timestamp of the first deviation created by the customer
        , min(
            case 
                when billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}'
                then deviation_created_at
                else null
                end
        ) as first_customer_deviation_created_at
        -- get the timestamp of the latest deviation created by the preselector or mealselector
        , max(
            case 
                when billing_agreement_basket_deviation_origin_id in 
                (
                    '{{ var("preselector_origin_id") }}'
                    ,'{{ var("mealselector_origin_id") }}'
                )
                then deviation_created_at
                else null
                end
        ) as last_recommendation_deviation_created_at
    from deviations
    group by 1,2

)

-- find the timestamp to be used when relating orders to billing agreements
-- to find the attributes of an billing agreement at the time they
-- started editing their order
, deviations_find_valid_at as (
    select 
        deviations_add_aggregated_columns.*
        , coalesce(last_recommendation_deviation_created_at, first_customer_deviation_created_at) as billing_agreement_valid_at
    from deviations_add_aggregated_columns

)

, join_tables as (
    select 
        deviations_find_related_order.billing_agreement_basket_id
        , deviations_find_related_order.menu_week_monday_date
        , deviations_find_related_order.billing_agreement_order_id
        , deviations_find_related_order.billing_agreement_id
        , deviations_find_related_order.billing_agreement_basket_deviation_origin_id
        , billing_agreements.company_id
        , deviations_find_valid_at.first_customer_deviation_created_at
        , deviations_find_valid_at.last_recommendation_deviation_created_at
        , deviations_find_valid_at.billing_agreement_valid_at
        , deviations_find_valid_at.is_onesub_migration
    from deviations_find_related_order
    left join billing_agreements
        on deviations_find_related_order.billing_agreement_id = billing_agreements.billing_agreement_id
    left join deviations_find_valid_at
        on deviations_find_related_order.billing_agreement_basket_id = deviations_find_valid_at.billing_agreement_basket_id
        and deviations_find_related_order.menu_week_monday_date = deviations_find_valid_at.menu_week_monday_date

)

select * from join_tables