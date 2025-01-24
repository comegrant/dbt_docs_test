with loyalty_orders as (
    
    select * from {{ ref('cms__loyalty_orders') }}

)

, loyalty_order_lines as (

    select * from {{ ref('cms__loyalty_order_lines') }}

)

, loyalty_order_lines_joined as (

    select
        loyalty_orders.loyalty_order_id
        , loyalty_orders.billing_agreement_id
        , loyalty_orders.loyalty_order_status_id
        , loyalty_orders.order_year
        , loyalty_orders.order_week
        , loyalty_orders.order_week_monday_date
        , loyalty_order_lines.loyalty_order_line_id
        , loyalty_order_lines.product_variation_id
        , loyalty_order_lines.unit_point_price
        , loyalty_order_lines.product_variation_quantity
        , loyalty_order_lines.total_point_price
        , loyalty_orders.source_created_at as loyalty_order_created_at
        , loyalty_orders.source_updated_at as loyalty_order_updated_at
    
    from loyalty_orders
    left join loyalty_order_lines 
        on loyalty_orders.loyalty_order_id = loyalty_order_lines.loyalty_order_id 

)

select * from loyalty_order_lines_joined