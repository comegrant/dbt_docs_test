with

weekly_recipe_ingredients as (

    select * from {{ ref('int_weekly_recipe_ingredients') }}
)

, products as (

    select * from {{ ref('dim_products') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, add_keys as (

    select 
    md5(concat_ws('-',
            weekly_recipe_ingredients.menu_variation_id,
            weekly_recipe_ingredients.menu_recipe_id,
            weekly_recipe_ingredients.recipe_portion_id,
            weekly_recipe_ingredients.ingredient_id
        )) as pk_fact_recipe_ingredients

    , weekly_recipe_ingredients.menu_variation_id
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

    , md5(weekly_recipe_ingredients.company_id) as fk_dim_companies
    , cast(date_format(weekly_recipe_ingredients.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates
    , products.pk_dim_products as fk_dim_products
    , md5(cast(concat(weekly_recipe_ingredients.recipe_id, weekly_recipe_ingredients.language_id) as string)) as fk_dim_recipes
    , portions_products.pk_dim_portions as fk_dim_portions
    , md5(concat_ws(
            '-'
            , weekly_recipe_ingredients.ingredient_id
            , weekly_recipe_ingredients.language_id
        )) as fk_dim_ingredients

    from weekly_recipe_ingredients

    left join products
        on weekly_recipe_ingredients.product_variation_id = products.product_variation_id
        and weekly_recipe_ingredients.company_id = products.company_id
    
    left join portions as portions_products
        on products.portion_name = portions_products.portion_name_local
        and weekly_recipe_ingredients.language_id = portions_products.language_id

)

select * from add_keys