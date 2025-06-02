with 

procurement_cycles as (

    select * from {{ ref('pim__procurement_cycles') }}

)

, weekly_menus as (

    select * from {{ ref('pim__weekly_menus') }}

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, suppliers as (

    select * from {{ ref('pim__ingredient_suppliers') }}

)

, ingredients as (

    select * from {{ ref('pim__ingredients') }}

)

, ingredient_prices as (

    select * from {{ ref('pim__ingredient_prices') }}

)

, weekly_purchase_orders as (

    select * from {{ ref('pim__weekly_purchase_orders') }}

)

, weekly_purchase_order_lines as (

    select * from {{ ref('pim__weekly_purchase_order_lines') }}

)

, weekly_purchase_order_lines_joined as (

    select
        weekly_purchase_orders.menu_year
        , weekly_purchase_orders.menu_week
        , weekly_purchase_order_lines.ingredient_id
        , coalesce(
                sum(ingredient_purchasing_cost * (original_ingredient_quantity + extra_ingredient_quantity - take_from_storage_ingredient_quantity)) 
                / nullif(sum(original_ingredient_quantity + extra_ingredient_quantity - take_from_storage_ingredient_quantity), 0)
                , 0
            ) as ingredient_average_purchasing_cost
    
    from weekly_purchase_orders

    left join weekly_purchase_order_lines
        on weekly_purchase_orders.purchase_order_id = weekly_purchase_order_lines.purchase_order_id

    where
        weekly_purchase_orders.purchase_order_status_id = 40 --only purchase orders with status 40 = "Ordered"
        and weekly_purchase_order_lines.purchase_order_line_status_id = 40 --only purchase order lines with status 40 = "Ordered"
    
    group by all
)

, ingredient_costs_joined as (

    select
        weekly_menus.menu_year
        , weekly_menus.menu_week
        , weekly_menus.company_id
        , procurement_cycles.purchasing_company_id --not really needed
        , purchasing_companies.country_id --not really needed
        , ingredients.ingredient_id
        , coalesce(
                campaign_prices.ingredient_unit_cost_markup
                , campaign_prices.ingredient_unit_cost
                , regular_prices.ingredient_unit_cost_markup
                , regular_prices.ingredient_unit_cost
            ) as ingredient_planned_cost
        , coalesce(campaign_prices.ingredient_unit_cost, regular_prices.ingredient_unit_cost) as ingredient_expected_cost
        , actual_costs.ingredient_average_purchasing_cost as ingredient_actual_cost
    
    from weekly_menus
    
    left join procurement_cycles
        on weekly_menus.company_id = procurement_cycles.company_id

    left join companies as purchasing_companies
        on procurement_cycles.purchasing_company_id = purchasing_companies.company_id
    
    left join suppliers
        on purchasing_companies.country_id = suppliers.country_id

    left join ingredients
        on suppliers.ingredient_supplier_id = ingredients.ingredient_supplier_id

    left join ingredient_prices as regular_prices
        on ingredients.ingredient_id = regular_prices.ingredient_id
        and weekly_menus.ingredient_purchase_date >= regular_prices.ingredient_price_valid_from
        and weekly_menus.ingredient_purchase_date <= regular_prices.ingredient_price_valid_to
        and regular_prices.ingredient_price_type_id = 0
    
    left join ingredient_prices as campaign_prices
        on ingredients.ingredient_id = campaign_prices.ingredient_id
        and weekly_menus.ingredient_purchase_date >= campaign_prices.ingredient_price_valid_from
        and weekly_menus.ingredient_purchase_date <= campaign_prices.ingredient_price_valid_to
        and campaign_prices.ingredient_price_type_id = 1

    left join weekly_purchase_order_lines_joined as actual_costs
        on ingredients.ingredient_id = actual_costs.ingredient_id
        and weekly_menus.menu_year = actual_costs.menu_year
        and weekly_menus.menu_week = actual_costs.menu_week

    where weekly_menus.menu_year >= 2019
        and regular_prices.ingredient_id is not null

)

select * from ingredient_costs_joined
