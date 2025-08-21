with 

weekly_purchase_orders as (

    select * from {{ ref('pim__weekly_purchase_orders') }}

)

, weekly_purchase_order_lines as (

    select * from {{ ref('pim__weekly_purchase_order_lines') }}

)

, ingredients as (

    select * from {{ref('pim__ingredients')}}

)

, weekly_purchase_order_lines_joined as (

    select
        weekly_purchase_orders.purchase_order_id
        , weekly_purchase_order_lines.purchase_order_line_id
        , weekly_purchase_orders.menu_year
        , weekly_purchase_orders.menu_week
        , weekly_purchase_orders.menu_week_monday_date
        , weekly_purchase_orders.purchasing_company_id
        , weekly_purchase_orders.is_special_purchase_order
        , weekly_purchase_orders.production_date
        , weekly_purchase_orders.purchase_delivery_date
        , weekly_purchase_orders.purchase_order_date
        , weekly_purchase_order_lines.ingredient_id
        , weekly_purchase_order_lines.original_ingredient_quantity
        , weekly_purchase_order_lines.ingredient_purchasing_cost
        , weekly_purchase_order_lines.vat
        , weekly_purchase_order_lines.total_purchasing_cost
        , weekly_purchase_order_lines.extra_ingredient_quantity 
        , weekly_purchase_order_lines.total_ingredient_quantity
        , weekly_purchase_order_lines.take_from_storage_ingredient_quantity
        , weekly_purchase_order_lines.received_ingredient_quantity
        , ingredients.ingredient_co2_emissions_per_unit

    from weekly_purchase_orders

    left join weekly_purchase_order_lines
        on weekly_purchase_orders.purchase_order_id = weekly_purchase_order_lines.purchase_order_id

    left join ingredients
        on weekly_purchase_order_lines.ingredient_id = ingredients.ingredient_id

    where
        weekly_purchase_orders.purchase_order_status_id = 40 --only purchase orders with status 40 = "Ordered"
        and weekly_purchase_order_lines.purchase_order_line_status_id = 40 --only purchase order lines with status 40 = "Ordered"
        and weekly_purchase_orders.menu_year >= 2019

    group by all
)

select * from weekly_purchase_order_lines_joined