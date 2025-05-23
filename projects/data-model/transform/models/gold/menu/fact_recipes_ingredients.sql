with

menu_weeks_recipes_portions as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, companies as (

    select * from {{ ref('dim_companies') }}

)

, products as (

    select * from {{ ref('dim_products') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, recipes as (

    select * from {{ ref('dim_recipes') }}

)

, dates as (

    select * from {{ ref('dim_dates') }}

)

, recipe_ingredients as (

    select * from {{ ref('int_recipe_ingredients_joined') }}

)

, ingredients as (

    select * from {{ ref('dim_ingredients') }}

)

, ingredient_prices as (

    select * from {{ ref('int_ingredient_prices_joined') }}

)

, all_tables_joined as (

    select
    -- These ids make up the primary key
        menu_weeks_recipes_portions.menu_variation_id
        , menu_weeks_recipes_portions.menu_recipe_id
        , menu_weeks_recipes_portions.recipe_portion_id
        , recipe_ingredients.ingredient_id

    -- Foreign keys:
        , companies.pk_dim_companies as fk_dim_companies
        , dates.pk_dim_dates as fk_dim_dates
        , products.pk_dim_products as fk_dim_products
        , recipes.pk_dim_recipes as fk_dim_recipes
        , portions_products.pk_dim_portions as fk_dim_portions
        , ingredients.pk_dim_ingredients as fk_dim_ingredients

    -- Additional information columns:
        , menu_weeks_recipes_portions.weekly_menu_id
        , menu_weeks_recipes_portions.company_id
        , menu_weeks_recipes_portions.language_id
        , menu_weeks_recipes_portions.menu_id
        , menu_weeks_recipes_portions.product_variation_id
        , menu_weeks_recipes_portions.recipe_id
        , recipes.main_recipe_variation_id
        , menu_weeks_recipes_portions.portion_id_menus
        , menu_weeks_recipes_portions.menu_year
        , menu_weeks_recipes_portions.menu_week
        , menu_weeks_recipes_portions.menu_week_monday_date
        , menu_weeks_recipes_portions.menu_number_days
        , menu_weeks_recipes_portions.menu_recipe_order
        , menu_weeks_recipes_portions.portion_name_products
        , menu_weeks_recipes_portions.is_selected_menu
        , menu_weeks_recipes_portions.is_locked_recipe

    -- The necessary columns:
    -- Because an ingredient might appear more than once in a recipe and this dataset should contain only one row per ingredient and recipe these columns have to be aggregated.
        , any(recipe_ingredients.is_recipe_main_carbohydrate) as is_recipe_main_carbohydrate
        , any(recipe_ingredients.is_recipe_main_protein) as is_recipe_main_protein
        , sum(recipe_ingredients.ingredient_nutrition_units) as ingredient_nutrition_units
        , sum(recipe_ingredients.ingredient_order_quantity) as ingredient_order_quantity
        , max(coalesce(ingredient_prices.ingredient_planned_cost, 0)) as ingredient_planned_cost
        , sum(coalesce(recipe_ingredients.ingredient_order_quantity, 0)) * max(coalesce(ingredient_prices.ingredient_planned_cost, 0)) as total_ingredient_planned_cost
        , ceiling(sum(coalesce(recipe_ingredients.ingredient_order_quantity, 0))) * max(coalesce(ingredient_prices.ingredient_planned_cost, 0)) as total_ingredient_planned_cost_whole_units
        , max(coalesce(ingredient_prices.ingredient_expected_cost, 0)) as ingredient_expected_cost
        , sum(coalesce(recipe_ingredients.ingredient_order_quantity, 0)) * max(coalesce(ingredient_prices.ingredient_expected_cost, 0)) as total_ingredient_expected_cost
        , ceiling(sum(coalesce(recipe_ingredients.ingredient_order_quantity, 0))) * max(coalesce(ingredient_prices.ingredient_expected_cost, 0)) as total_ingredient_expected_cost_whole_units
        , max(coalesce(ingredient_prices.ingredient_actual_cost, 0)) as ingredient_actual_cost
        , sum(coalesce(recipe_ingredients.ingredient_order_quantity, 0)) * max(coalesce(ingredient_prices.ingredient_actual_cost, 0)) as total_ingredient_actual_cost
        , ceiling(sum(coalesce(recipe_ingredients.ingredient_order_quantity, 0))) * max(coalesce(ingredient_prices.ingredient_actual_cost, 0)) as total_ingredient_actual_cost_whole_units
    
    from menu_weeks_recipes_portions

    left join dates
        on menu_weeks_recipes_portions.menu_week_monday_date = dates.date

    left join products
        on menu_weeks_recipes_portions.product_variation_id = products.product_variation_id
        and menu_weeks_recipes_portions.company_id = products.company_id

    left join companies
        on menu_weeks_recipes_portions.company_id = companies.company_id
    
    left join portions as portions_products
        on products.portion_name = portions_products.portion_name_local
        and companies.language_id = portions_products.language_id

    left join recipes
        on menu_weeks_recipes_portions.recipe_id = recipes.recipe_id
        and menu_weeks_recipes_portions.language_id = recipes.language_id

    left join recipe_ingredients
        on menu_weeks_recipes_portions.recipe_id = recipe_ingredients.recipe_id
        and menu_weeks_recipes_portions.portion_id_menus = recipe_ingredients.portion_id
        and menu_weeks_recipes_portions.language_id = recipe_ingredients.language_id

    left join ingredients
        on recipe_ingredients.ingredient_id = ingredients.ingredient_id
        and menu_weeks_recipes_portions.language_id = ingredients.language_id

    left join ingredient_prices
        on menu_weeks_recipes_portions.menu_year = ingredient_prices.menu_year
        and menu_weeks_recipes_portions.menu_week = ingredient_prices.menu_week
        and menu_weeks_recipes_portions.company_id = ingredient_prices.company_id
        and recipe_ingredients.ingredient_id = ingredient_prices.ingredient_id

    where menu_weeks_recipes_portions.recipe_id is not null
        and recipe_ingredients.ingredient_id is not null
    
    group by all

)

, all_tables_joined_with_pk as (

    select
        md5(concat_ws('-',
            all_tables_joined.menu_variation_id,
            all_tables_joined.menu_recipe_id,
            all_tables_joined.recipe_portion_id,
            all_tables_joined.ingredient_id
        )) as pk_fact_recipes_ingredients
        , all_tables_joined.*
    
    from all_tables_joined

)

select * from all_tables_joined_with_pk