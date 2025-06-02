with

menu_weeks as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, ingredient_costs as (

    select * from {{ ref('fact_recipes_ingredients') }}

)

, price_categories as (

    select * from {{ ref('pim__price_categories') }}

)

, recipe_costs as (

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

        , sum(ingredient_costs.total_ingredient_planned_cost) as recipe_planned_cost
        , sum(ingredient_costs.total_ingredient_planned_cost_whole_units) as recipe_planned_cost_whole_units
        , sum(ingredient_costs.total_ingredient_expected_cost) as recipe_expected_cost
        , sum(ingredient_costs.total_ingredient_expected_cost_whole_units) as recipe_expected_cost_whole_units
        , sum(ingredient_costs.total_ingredient_actual_cost) as recipe_actual_cost
        , sum(ingredient_costs.total_ingredient_actual_cost_whole_units) as recipe_actual_cost_whole_units

    from menu_weeks

    left join portions
        on menu_weeks.portion_name_products = portions.portion_name_local
        and menu_weeks.language_id = portions.language_id

    left join ingredient_costs
        on menu_weeks.company_id = ingredient_costs.company_id
        and menu_weeks.menu_year = ingredient_costs.menu_year
        and menu_weeks.menu_week = ingredient_costs.menu_week
        and menu_weeks.recipe_id = ingredient_costs.recipe_id

    group by all

)

, add_price_categories_and_keys as (
    select
        md5(concat_ws('-',
            recipe_costs.weekly_menu_id,
            recipe_costs.menu_id,
            recipe_costs.product_variation_id,
            recipe_costs.recipe_id,
            recipe_costs.menu_recipe_id,
            recipe_costs.portion_id,
            recipe_costs.menu_number_days,
            recipe_costs.menu_recipe_order
        )) as pk_fact_menus

        , recipe_costs.weekly_menu_id
        , recipe_costs.company_id
        , recipe_costs.language_id
        , recipe_costs.menu_id
        , recipe_costs.menu_variation_id
        , recipe_costs.product_variation_id
        , recipe_costs.menu_recipe_id
        , recipe_costs.recipe_id

        , recipe_costs.recipe_portion_id
        , recipe_costs.portion_id_menus
        , recipe_costs.portion_id_products
        , recipe_costs.portion_id

        , recipe_costs.menu_year
        , recipe_costs.menu_week
        , recipe_costs.menu_week_monday_date
        , recipe_costs.menu_week_financial_date

        , recipe_costs.menu_number_days
        , recipe_costs.menu_recipe_order

        , recipe_costs.is_locked_recipe
        , recipe_costs.is_selected_menu
        , recipe_costs.is_dish
        , recipe_costs.is_future_menu_week

        , recipe_costs.recipe_planned_cost
        , recipe_costs.recipe_planned_cost_whole_units
        , recipe_costs.recipe_expected_cost
        , recipe_costs.recipe_expected_cost_whole_units
        , recipe_costs.recipe_actual_cost
        , recipe_costs.recipe_actual_cost_whole_units

        , price_categories.price_category_level_id
        , price_categories.price_category_level_name
        , price_categories.price_category_price_inc_vat

        {# FKS #}
        , md5(recipe_costs.company_id) as fk_dim_companies
        , cast(date_format(recipe_costs.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(concat(recipe_costs.portion_id, recipe_costs.language_id)) as fk_dim_portions
        , md5(concat(recipe_costs.product_variation_id, recipe_costs.company_id)) as fk_dim_products
        , coalesce(md5(cast(concat(recipe_costs.recipe_id, recipe_costs.language_id) as string)),0) as fk_dim_recipes

    from recipe_costs

    left join price_categories
        on recipe_costs.company_id = price_categories.company_id
        and recipe_costs.portion_id = price_categories.portion_id
        and recipe_costs.recipe_planned_cost_whole_units >= price_category_min_total_ingredient_cost
        and recipe_costs.recipe_planned_cost_whole_units < price_categories.price_category_max_total_ingredient_cost
        and recipe_costs.menu_week_monday_date >= price_categories.price_category_valid_from
        and recipe_costs.menu_week_monday_date <= price_categories.price_category_valid_to

)

select * from add_price_categories_and_keys
