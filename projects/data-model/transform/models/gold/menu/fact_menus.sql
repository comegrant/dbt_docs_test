with

menu_weeks as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, recipe_costs_and_co2 as (

    select * from {{ ref('int_weekly_recipe_costs_and_co2') }}

)

, ingredient_combinations as (

    select * from {{ ref('int_recipes_with_ingredient_combinations') }}

)

, price_categories as (

    select * from {{ ref('pim__price_categories') }}

)

, add_recipe_costs_and_co2 as (

    select
        menu_weeks.weekly_menu_id
        , menu_weeks.company_id
        , menu_weeks.language_id
        , menu_weeks.menu_id
        , menu_weeks.menu_variation_id
        , menu_weeks.product_variation_id
        , menu_weeks.menu_recipe_id
        , menu_weeks.recipe_id

        , menu_weeks.recipe_portion_id
        , menu_weeks.portion_id_menus
        , portions.portion_id as portion_id_products
        -- There are cases before 2023-05-08 where the portion_id from the product variations and the menu variations deviates.
        -- In those cases we will use the portion_id from the product variations.
        -- In cases where portions_products.portion_id is null we will use the portion_id from the menu variations.
        -- We will catch new cases from tests and try to get them fixed in the source systems.
        , coalesce(portions.portion_id, menu_weeks.portion_id_menus) as portion_id

        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , menu_weeks.menu_week_monday_date
        , menu_weeks.menu_week_financial_date

        , menu_weeks.menu_number_days
        , menu_weeks.menu_recipe_order

        , menu_weeks.is_locked_recipe
        , menu_weeks.is_selected_menu
        , menu_weeks.is_dish
        , menu_weeks.is_future_menu_week

        , recipe_costs_and_co2.total_ingredient_weight
        , recipe_costs_and_co2.total_ingredient_weight_whole_units

        , recipe_costs_and_co2.total_ingredient_planned_cost
        , recipe_costs_and_co2.total_ingredient_planned_cost_whole_units

        , recipe_costs_and_co2.total_ingredient_expected_cost
        , recipe_costs_and_co2.total_ingredient_expected_cost_whole_units

        , recipe_costs_and_co2.total_ingredient_actual_cost
        , recipe_costs_and_co2.total_ingredient_actual_cost_whole_units

        , recipe_costs_and_co2.total_ingredient_co2_emissions
        , recipe_costs_and_co2.total_ingredient_co2_emissions_whole_units

        , recipe_costs_and_co2.total_ingredient_weight_with_co2_data
        , recipe_costs_and_co2.total_ingredient_weight_with_co2_data_whole_units

    from menu_weeks

    left join portions
        on menu_weeks.portion_name_products = portions.portion_name_local
        and menu_weeks.language_id = portions.language_id

    left join recipe_costs_and_co2
        on menu_weeks.company_id = recipe_costs_and_co2.company_id
        and menu_weeks.menu_year = recipe_costs_and_co2.menu_year
        and menu_weeks.menu_week = recipe_costs_and_co2.menu_week
        and menu_weeks.recipe_id = recipe_costs_and_co2.recipe_id
        and menu_weeks.recipe_portion_id = recipe_costs_and_co2.recipe_portion_id

    group by all

)

, add_price_categories_and_keys as (
    select
        md5(concat_ws('-',
            add_recipe_costs_and_co2.weekly_menu_id,
            add_recipe_costs_and_co2.menu_id,
            add_recipe_costs_and_co2.product_variation_id,
            add_recipe_costs_and_co2.recipe_id,
            add_recipe_costs_and_co2.menu_recipe_id,
            add_recipe_costs_and_co2.portion_id,
            add_recipe_costs_and_co2.menu_number_days,
            add_recipe_costs_and_co2.menu_recipe_order
        )) as pk_fact_menus

        , add_recipe_costs_and_co2.weekly_menu_id
        , add_recipe_costs_and_co2.company_id
        , add_recipe_costs_and_co2.language_id
        , add_recipe_costs_and_co2.menu_id
        , add_recipe_costs_and_co2.menu_variation_id
        , add_recipe_costs_and_co2.product_variation_id
        , add_recipe_costs_and_co2.menu_recipe_id
        , add_recipe_costs_and_co2.recipe_id

        , add_recipe_costs_and_co2.recipe_portion_id
        , add_recipe_costs_and_co2.portion_id_menus
        , add_recipe_costs_and_co2.portion_id_products
        , add_recipe_costs_and_co2.portion_id
        , ingredient_combinations.ingredient_combination_id

        , add_recipe_costs_and_co2.menu_year
        , add_recipe_costs_and_co2.menu_week
        , add_recipe_costs_and_co2.menu_week_monday_date
        , add_recipe_costs_and_co2.menu_week_financial_date

        , add_recipe_costs_and_co2.menu_number_days
        , add_recipe_costs_and_co2.menu_recipe_order

        , add_recipe_costs_and_co2.is_locked_recipe
        , add_recipe_costs_and_co2.is_selected_menu
        , add_recipe_costs_and_co2.is_dish
        , add_recipe_costs_and_co2.is_future_menu_week

        , add_recipe_costs_and_co2.total_ingredient_weight
        , add_recipe_costs_and_co2.total_ingredient_weight_whole_units

        , add_recipe_costs_and_co2.total_ingredient_planned_cost
        , add_recipe_costs_and_co2.total_ingredient_planned_cost_whole_units
        
        , add_recipe_costs_and_co2.total_ingredient_expected_cost
        , add_recipe_costs_and_co2.total_ingredient_expected_cost_whole_units
        
        , add_recipe_costs_and_co2.total_ingredient_actual_cost
        , add_recipe_costs_and_co2.total_ingredient_actual_cost_whole_units

        , add_recipe_costs_and_co2.total_ingredient_co2_emissions
        , add_recipe_costs_and_co2.total_ingredient_co2_emissions_whole_units
        
        , add_recipe_costs_and_co2.total_ingredient_weight_with_co2_data
        , add_recipe_costs_and_co2.total_ingredient_weight_with_co2_data_whole_units

        , price_categories.price_category_level_id
        , price_categories.price_category_level_name
        , price_categories.price_category_price_inc_vat

        {# FKS #}
        , md5(add_recipe_costs_and_co2.company_id) as fk_dim_companies
        , cast(date_format(add_recipe_costs_and_co2.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(concat(add_recipe_costs_and_co2.portion_id, add_recipe_costs_and_co2.language_id)) as fk_dim_portions
        , md5(concat(add_recipe_costs_and_co2.product_variation_id, add_recipe_costs_and_co2.company_id)) as fk_dim_products
        , coalesce(md5(cast(concat(add_recipe_costs_and_co2.recipe_id, add_recipe_costs_and_co2.language_id) as string)),0) as fk_dim_recipes
        , coalesce(md5(concat(ingredient_combinations.ingredient_combination_id, add_recipe_costs_and_co2.language_id)), '0') as fk_dim_ingredient_combinations

    from add_recipe_costs_and_co2

    left join price_categories
        on add_recipe_costs_and_co2.company_id = price_categories.company_id
        and add_recipe_costs_and_co2.portion_id = price_categories.portion_id
        and add_recipe_costs_and_co2.total_ingredient_planned_cost_whole_units >= price_category_min_total_ingredient_cost
        and add_recipe_costs_and_co2.total_ingredient_planned_cost_whole_units < price_categories.price_category_max_total_ingredient_cost
        and add_recipe_costs_and_co2.menu_week_monday_date >= price_categories.price_category_valid_from
        and add_recipe_costs_and_co2.menu_week_monday_date <= price_categories.price_category_valid_to

    left join ingredient_combinations
        on add_recipe_costs_and_co2.recipe_id = ingredient_combinations.recipe_id
        and add_recipe_costs_and_co2.portion_id = ingredient_combinations.portion_id
        and add_recipe_costs_and_co2.language_id = ingredient_combinations.language_id

)

select * from add_price_categories_and_keys
