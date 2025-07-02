with

orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}

)

, order_lines as (

    select * from {{ ref('cms__billing_agreement_order_lines') }}

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, order_lines_joined as (

    select
        billing_agreements.company_id
        , orders.billing_agreement_id
        , orders.billing_agreement_order_id
        , orders.ops_order_id
        , orders.order_status_id
        , orders.order_type_id
        , orders.menu_year
        , orders.menu_week
        , orders.menu_week_monday_date
        , orders.source_created_at
        , orders.has_recipe_leaflets
        , order_lines.product_variation_quantity
        , order_lines.vat
        , order_lines.unit_price_ex_vat
        , order_lines.unit_price_inc_vat
        , order_lines.total_amount_ex_vat
        , order_lines.total_amount_inc_vat
        , order_lines.order_line_type_name
        , order_lines.billing_agreement_order_line_id
        , order_lines.product_variation_id
    from orders
    left join order_lines
        on orders.billing_agreement_order_id = order_lines.billing_agreement_order_id
    left join billing_agreements
        on orders.billing_agreement_id = billing_agreements.billing_agreement_id
    -- Exclude orders without any order lines
    where billing_agreement_order_line_id is not null

)

select * from order_lines_joined
