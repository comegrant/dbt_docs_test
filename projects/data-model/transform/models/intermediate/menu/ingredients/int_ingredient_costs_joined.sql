with 

weekly_purchase_orders as (

    select * from {{ ref('int_weekly_purchase_order_lines_joined')}}

)

, weekly_menus as (

    select * from {{ ref('pim__weekly_menus') }}

)

, ingredients as (

    select * from {{ ref('pim__ingredients') }}

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, procurement_cycles as (

    select * from {{ ref('pim__procurement_cycles') }}

)

, ingredient_prices as (

    select * from {{ ref('pim__ingredient_prices') }}

)

-- When there is no purchase order for an ingredient for a specific menu week
-- then the actual cost must be fetched from the latest previous purchase order
-- 1. Add the date of the Wednesday before the menu week to join with the default purchasing date in weekly_menus
--    and create a row number for each ingredient based on the menu week
, weekly_purchase_orders_get_unit_costs_ordered as (

    select
        menu_year
        , menu_week
        , purchasing_company_id
        , dateadd(day,-5, {{ get_iso_week_start_date('menu_year', 'menu_week') }}) as wednesday_before_menu_week
        , ingredient_id
        , coalesce(
                sum(ingredient_purchasing_cost * (original_ingredient_quantity + extra_ingredient_quantity - take_from_storage_ingredient_quantity)) 
                / nullif(sum(original_ingredient_quantity + extra_ingredient_quantity - take_from_storage_ingredient_quantity), 0)
                , 0
            ) as ingredient_actual_cost_per_unit
        , row_number() over (partition by ingredient_id order by (weekly_purchase_orders.menu_year*100 + weekly_purchase_orders.menu_week) ) as ingredient_week_sequence_nr
    
    from weekly_purchase_orders

    group by
        menu_year
        , menu_week
        , purchasing_company_id
        , ingredient_id

)

-- 2. Take all menu weeks from weekly_menus and for each menu week and ingredient
--    find the greatest row number from the purchasing orders for any week before or on the actual menu week
, find_latest_relevant_purchasing_orders as (

    select
        weekly_menus.menu_week_monday_date
        , weekly_menus.ingredient_purchase_date
        , weekly_menus.company_id
        , weekly_purchase_orders_get_unit_costs_ordered.purchasing_company_id
        , companies.country_id
        , weekly_purchase_orders_get_unit_costs_ordered.ingredient_id
        , max(weekly_purchase_orders_get_unit_costs_ordered.ingredient_week_sequence_nr) as max_ingredient_week_sequence_nr
        
        from weekly_menus

        left join companies
            on weekly_menus.company_id = companies.company_id

        left join procurement_cycles
            on weekly_menus.company_id = procurement_cycles.company_id

        left join weekly_purchase_orders_get_unit_costs_ordered
            on procurement_cycles.purchasing_company_id = weekly_purchase_orders_get_unit_costs_ordered.purchasing_company_id
            and weekly_menus.ingredient_purchase_date >= weekly_purchase_orders_get_unit_costs_ordered.wednesday_before_menu_week

    group by all

)

--3. Get the lastest relevant actual cost for all menu weeks based on the max row number from the previous CTE
, ingredient_relevant_actual_costs_joined as (

    select
        find_latest_relevant_purchasing_orders.menu_week_monday_date
        , find_latest_relevant_purchasing_orders.ingredient_purchase_date
        , find_latest_relevant_purchasing_orders.company_id
        , find_latest_relevant_purchasing_orders.purchasing_company_id
        , find_latest_relevant_purchasing_orders.country_id
        , find_latest_relevant_purchasing_orders.ingredient_id
        , weekly_purchase_orders_get_unit_costs_ordered.ingredient_actual_cost_per_unit
        , weekly_purchase_orders_get_unit_costs_ordered.wednesday_before_menu_week
    
    from find_latest_relevant_purchasing_orders

    left join weekly_purchase_orders_get_unit_costs_ordered
        on find_latest_relevant_purchasing_orders.ingredient_id = weekly_purchase_orders_get_unit_costs_ordered.ingredient_id
        and find_latest_relevant_purchasing_orders.ingredient_purchase_date >= weekly_purchase_orders_get_unit_costs_ordered.wednesday_before_menu_week
        and find_latest_relevant_purchasing_orders.max_ingredient_week_sequence_nr = weekly_purchase_orders_get_unit_costs_ordered.ingredient_week_sequence_nr
        and find_latest_relevant_purchasing_orders.purchasing_company_id = weekly_purchase_orders_get_unit_costs_ordered.purchasing_company_id

)


, add_costs_to_weekly_ingredients as (

    select

        -- company_id + menu_week_monday_date + ingredient_id represents the unique key of this model
        weekly_menus.company_id
        , weekly_menus.menu_week_monday_date
        , ingredients.ingredient_id

        -- purchasing information
        , ingredient_relevant_actual_costs_joined.purchasing_company_id
        , ingredient_relevant_actual_costs_joined.country_id

        --unit costs
        , coalesce(
                campaign_prices.ingredient_unit_cost_markup
                , campaign_prices.ingredient_unit_cost
                , regular_prices.ingredient_unit_cost_markup
                , regular_prices.ingredient_unit_cost
            ) as ingredient_planned_cost_per_unit
        , coalesce(campaign_prices.ingredient_unit_cost, regular_prices.ingredient_unit_cost) as ingredient_expected_cost_per_unit
        , ingredient_relevant_actual_costs_joined.ingredient_actual_cost_per_unit
    
    from weekly_menus

    cross join ingredients
    
    left join ingredient_relevant_actual_costs_joined
        on ingredients.ingredient_id = ingredient_relevant_actual_costs_joined.ingredient_id
        and weekly_menus.ingredient_purchase_date = ingredient_relevant_actual_costs_joined.ingredient_purchase_date
        and weekly_menus.menu_week_monday_date = ingredient_relevant_actual_costs_joined.menu_week_monday_date
        and weekly_menus.company_id = ingredient_relevant_actual_costs_joined.company_id

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

    where regular_prices.ingredient_id is not null

)

select * from add_costs_to_weekly_ingredients
