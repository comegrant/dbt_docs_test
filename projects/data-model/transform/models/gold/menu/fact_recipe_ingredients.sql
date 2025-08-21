with

weekly_recipe_ingredients as (

    select * from {{ ref('int_weekly_recipe_ingredients') }}
)

, recipe_costs as (

    select * from {{ ref('int_weekly_recipe_costs_and_co2') }}

)

, products as (

    select * from {{ ref('dim_products') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, price_categories as (

    select * from {{ ref('dim_price_categories') }}

)

, orders as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, orders_aggregated as (
    select 
        menu_week_monday_date
        , company_id
        , product_variation_id
        , sum(product_variation_quantity) as product_variation_quantity
        , count(distinct billing_agreement_order_id) as number_of_orders
    from orders
    where 
        menu_year > 2023
    group by all
)

, add_ordered_ingredient_numbers as (

    select 

    weekly_recipe_ingredients.menu_variation_id
    , weekly_recipe_ingredients.menu_recipe_id
    , weekly_recipe_ingredients.recipe_portion_id
    , weekly_recipe_ingredients.ingredient_id
    , weekly_recipe_ingredients.ingredient_internal_reference
    , weekly_recipe_ingredients.weekly_menu_id
    , weekly_recipe_ingredients.company_id
    , weekly_recipe_ingredients.language_id
    , weekly_recipe_ingredients.menu_id
    , weekly_recipe_ingredients.product_variation_id
    , weekly_recipe_ingredients.recipe_id
    , weekly_recipe_ingredients.main_recipe_variation_id
    , weekly_recipe_ingredients.portion_id_menus
    , weekly_recipe_ingredients.menu_year
    , weekly_recipe_ingredients.menu_week
    , weekly_recipe_ingredients.menu_week_monday_date
    , weekly_recipe_ingredients.menu_number_days
    , weekly_recipe_ingredients.menu_recipe_order
    , weekly_recipe_ingredients.portion_name_products
    , weekly_recipe_ingredients.is_selected_menu
    , weekly_recipe_ingredients.is_locked_recipe
    , weekly_recipe_ingredients.is_main_carbohydrate
    , weekly_recipe_ingredients.is_main_protein
    , weekly_recipe_ingredients.nutrition_units

    , weekly_recipe_ingredients.ingredient_order_quantity
    
    , weekly_recipe_ingredients.ingredient_weight_per_unit
    , weekly_recipe_ingredients.total_ingredient_weight
    , weekly_recipe_ingredients.total_ingredient_weight_whole_units

    , weekly_recipe_ingredients.ingredient_planned_cost_per_unit
    , weekly_recipe_ingredients.total_ingredient_planned_cost
    , weekly_recipe_ingredients.total_ingredient_planned_cost_whole_units

    , weekly_recipe_ingredients.ingredient_expected_cost_per_unit
    , weekly_recipe_ingredients.total_ingredient_expected_cost
    , weekly_recipe_ingredients.total_ingredient_expected_cost_whole_units

    , weekly_recipe_ingredients.ingredient_actual_cost_per_unit
    , weekly_recipe_ingredients.total_ingredient_actual_cost
    , weekly_recipe_ingredients.total_ingredient_actual_cost_whole_units

    , weekly_recipe_ingredients.ingredient_co2_emissions_per_unit
    , weekly_recipe_ingredients.total_ingredient_co2_emissions
    , weekly_recipe_ingredients.total_ingredient_co2_emissions_whole_units

    , weekly_recipe_ingredients.ingredient_weight_with_co2_data
    , weekly_recipe_ingredients.total_ingredient_weight_with_co2_data
    , weekly_recipe_ingredients.total_ingredient_weight_with_co2_data_whole_units

    , orders_aggregated.product_variation_quantity as customer_ordered_product_variation_quantity
    , orders_aggregated.number_of_orders as number_of_customer_orders

    , weekly_recipe_ingredients.ingredient_order_quantity * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_quantity
    
    , weekly_recipe_ingredients.total_ingredient_weight  * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_weight
    , weekly_recipe_ingredients.total_ingredient_weight_whole_units * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_weight_whole_units

    , weekly_recipe_ingredients.total_ingredient_planned_cost * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_planned_cost
    , weekly_recipe_ingredients.total_ingredient_planned_cost_whole_units * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_planned_cost_whole_units

    , weekly_recipe_ingredients.total_ingredient_expected_cost * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_expected_cost
    , weekly_recipe_ingredients.total_ingredient_expected_cost_whole_units * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_expected_cost_whole_units

    , weekly_recipe_ingredients.total_ingredient_actual_cost * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_actual_cost
    , weekly_recipe_ingredients.total_ingredient_actual_cost_whole_units * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_actual_cost_whole_units

    , weekly_recipe_ingredients.total_ingredient_co2_emissions * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_co2_emissions
    , weekly_recipe_ingredients.total_ingredient_co2_emissions_whole_units * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_co2_emissions_whole_units

    , weekly_recipe_ingredients.total_ingredient_weight_with_co2_data * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_weight_with_co2_data
    , weekly_recipe_ingredients.total_ingredient_weight_with_co2_data_whole_units * coalesce(orders_aggregated.product_variation_quantity,0) as customer_ordered_ingredient_weight_with_co2_data_whole_units

    from weekly_recipe_ingredients

    left join orders_aggregated
        on weekly_recipe_ingredients.menu_week_monday_date = orders_aggregated.menu_week_monday_date
        and weekly_recipe_ingredients.company_id = orders_aggregated.company_id
        and weekly_recipe_ingredients.product_variation_id = orders_aggregated.product_variation_id

)

, add_keys as (

    select 
    md5(concat_ws('-',
            add_ordered_ingredient_numbers.menu_variation_id,
            add_ordered_ingredient_numbers.menu_recipe_id,
            add_ordered_ingredient_numbers.recipe_portion_id,
            add_ordered_ingredient_numbers.ingredient_id
        )) as pk_fact_recipe_ingredients

    , add_ordered_ingredient_numbers.*

    , md5(add_ordered_ingredient_numbers.company_id) as fk_dim_companies
    , cast(date_format(add_ordered_ingredient_numbers.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates
    , products.pk_dim_products as fk_dim_products
    , md5(cast(concat(add_ordered_ingredient_numbers.recipe_id, add_ordered_ingredient_numbers.language_id) as string)) as fk_dim_recipes
    , portions_products.pk_dim_portions as fk_dim_portions
    , md5(concat_ws(
            '-'
            , add_ordered_ingredient_numbers.ingredient_id
            , add_ordered_ingredient_numbers.language_id
        )) as fk_dim_ingredients
    , coalesce(price_categories.pk_dim_price_categories, '0') as fk_dim_price_categories

    from add_ordered_ingredient_numbers

    left join products
        on add_ordered_ingredient_numbers.product_variation_id = products.product_variation_id
        and add_ordered_ingredient_numbers.company_id = products.company_id

    left join portions as portions_products
        on products.portion_name = portions_products.portion_name_local
        and add_ordered_ingredient_numbers.language_id = portions_products.language_id

    left join recipe_costs
        on add_ordered_ingredient_numbers.company_id = recipe_costs.company_id
        and add_ordered_ingredient_numbers.menu_week_monday_date = recipe_costs.menu_week_monday_date
        and add_ordered_ingredient_numbers.product_variation_id = recipe_costs.product_variation_id
        and add_ordered_ingredient_numbers.recipe_id = recipe_costs.recipe_id

    left join price_categories
        on add_ordered_ingredient_numbers.company_id = price_categories.company_id
        and portions_products.portion_id = price_categories.portion_id
        and recipe_costs.total_ingredient_planned_cost_whole_units >= price_categories.min_ingredient_cost_inc_vat
        and recipe_costs.total_ingredient_planned_cost_whole_units < price_categories.max_ingredient_cost_inc_vat
        and add_ordered_ingredient_numbers.menu_week_monday_date >= price_categories.valid_from
        and add_ordered_ingredient_numbers.menu_week_monday_date < price_categories.valid_to

)

select * from add_keys