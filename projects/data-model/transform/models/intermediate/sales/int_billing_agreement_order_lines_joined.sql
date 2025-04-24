with

orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}

)

, order_lines as (

    select * from {{ ref('cms__billing_agreement_order_lines') }}

)

, has_delivery as (

    select * from {{ ref('int_billing_agreement_order_lines_with_delivery') }}

)

, order_lines_joined as (

    select
        orders.menu_year
        , orders.menu_week
        , orders.menu_week_monday_date
        , {{ get_financial_date_from_monday_date('orders.menu_week_monday_date') }} as menu_week_financial_date
        , orders.source_created_at
        , order_lines.product_variation_quantity
        , order_lines.vat
        , order_lines.unit_price_ex_vat
        , order_lines.unit_price_inc_vat
        , order_lines.total_amount_ex_vat
        , order_lines.total_amount_inc_vat
        , order_lines.order_line_type_name
        , orders.has_recipe_leaflets
        , coalesce(has_delivery.has_delivery, false) as has_delivery
        , orders.billing_agreement_id
        , orders.billing_agreement_order_id
        , orders.ops_order_id
        , orders.order_status_id
        , orders.order_type_id
        , order_lines.billing_agreement_order_line_id
        , order_lines.product_variation_id
    from orders
    left join order_lines
        on orders.billing_agreement_order_id = order_lines.billing_agreement_order_id
    left join has_delivery
        on orders.billing_agreement_order_id = has_delivery.billing_agreement_order_id
    -- Exclude orders without any order lines
    where billing_agreement_order_line_id is not null

)

select * from order_lines_joined
