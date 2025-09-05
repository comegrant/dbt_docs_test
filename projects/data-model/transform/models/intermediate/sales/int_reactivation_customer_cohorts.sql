with 
customer_journey_segments as (
    select * from {{ ref('int_customer_journey_segments') }}

)

, billing_agreement_order_discounts as (
    select * from {{ ref('cms__billing_agreement_order_discounts') }} 
    
)

, billing_agreement_orders as (
    select * from {{ ref('cms__billing_agreement_orders') }} 
    
)

, discounts as (
        select * from {{ ref('cms__discounts') }} 

)

, customer_journey_reactivations as (
    select billing_agreement_id
        , menu_week_monday_date_from
        , menu_week_cutoff_at_from
    from customer_journey_segments
    where sub_segment_id = {{ var("customer_journey_sub_segment_ids")["reactivated"] }}

)

, discounts_and_orders_joined as (

    select billing_agreement_orders.billing_agreement_id
    , billing_agreement_orders.menu_week_monday_date
    , billing_agreement_order_discounts.discount_id
    , discounts.discount_title
    from billing_agreement_order_discounts
    left join billing_agreement_orders on billing_agreement_orders.billing_agreement_order_id = billing_agreement_order_discounts.billing_agreement_order_id
    left join discounts on billing_agreement_order_discounts.discount_id = discounts.discount_id
    where customer_usage_limit != -1

)

, discounts_and_orders_joined_ranked as (

    select * from 
        (
        select billing_agreement_id
        , menu_week_monday_date
        , discount_id
        , discount_title
        , row_number() over (
            partition by billing_agreement_id, menu_week_monday_date
            order by discount_id
            )
        as row_number
        from discounts_and_orders_joined
        )
    where row_number = 1

)

, reactivations_with_valid_from_and_to as (

    select
        billing_agreement_id
        , menu_week_monday_date_from as menu_week_monday_date_from
        , menu_week_cutoff_at_from as menu_week_cutoff_at_from
        , {{ get_scd_valid_to('menu_week_monday_date_from ', 'billing_agreement_id') }} as menu_week_monday_date_to
        , {{ get_scd_valid_to('menu_week_cutoff_at_from ', 'billing_agreement_id') }} as menu_week_cutoff_at_to
        from customer_journey_reactivations

)

, reactivations_with_discount as (

    select
        reactivations_with_valid_from_and_to.billing_agreement_id
        , menu_week_monday_date_from
        , menu_week_cutoff_at_from
        , menu_week_monday_date_to
        , menu_week_cutoff_at_to
        , discount_title
    from reactivations_with_valid_from_and_to
    left join discounts_and_orders_joined_ranked 
        on reactivations_with_valid_from_and_to.menu_week_monday_date_from = discounts_and_orders_joined_ranked.menu_week_monday_date
        and reactivations_with_valid_from_and_to.billing_agreement_id = discounts_and_orders_joined_ranked.billing_agreement_id

)

select * from reactivations_with_discount
