with

menu_weeks_recipes_portions as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, recipe_ingredients as (

    select * from {{ ref('int_recipe_ingredients_joined') }}

)

, ingredient_prices as (

    select * from {{ ref('int_ingredient_costs_joined') }}

)

, recipes as (

    select * from {{ ref('pim__recipes') }}

)

, all_tables_joined as (

    select
        -- These ids make up the unique key of the table
        menu_weeks_recipes_portions.menu_variation_id
        , menu_weeks_recipes_portions.menu_recipe_id
        , menu_weeks_recipes_portions.recipe_portion_id
        , recipe_ingredients.ingredient_id
        , recipe_ingredients.ingredient_internal_reference

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
        , any(recipe_ingredients.is_main_carbohydrate) as is_main_carbohydrate
        , any(recipe_ingredients.is_main_protein) as is_main_protein
        , sum(recipe_ingredients.nutrition_units) as nutrition_units
        , sum(recipe_ingredients.recipe_ingredient_quantity) as recipe_ingredient_quantity
        
        , any_value(coalesce(recipe_ingredients.ingredient_net_weight, 0)) as ingredient_weight_per_unit
        , sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0)) * any_value(coalesce(recipe_ingredients.ingredient_net_weight, 0)) as total_ingredient_weight
        , ceiling(sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0))) * any_value(coalesce(recipe_ingredients.ingredient_net_weight, 0)) as total_ingredient_weight_whole_units

        , any_value(coalesce(ingredient_prices.ingredient_planned_cost_per_unit, 0)) as ingredient_planned_cost_per_unit
        , sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0)) * any_value(coalesce(ingredient_prices.ingredient_planned_cost_per_unit, 0)) as total_ingredient_planned_cost
        , ceiling(sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0))) * any_value(coalesce(ingredient_prices.ingredient_planned_cost_per_unit, 0)) as total_ingredient_planned_cost_whole_units
        
        , any_value(coalesce(ingredient_prices.ingredient_expected_cost_per_unit, 0)) as ingredient_expected_cost_per_unit
        , sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0)) * any_value(coalesce(ingredient_prices.ingredient_expected_cost_per_unit, 0)) as total_ingredient_expected_cost
        , ceiling(sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0))) * any_value(coalesce(ingredient_prices.ingredient_expected_cost_per_unit, 0)) as total_ingredient_expected_cost_whole_units
        
        , any_value(coalesce(ingredient_prices.ingredient_actual_cost_per_unit, 0)) as ingredient_actual_cost_per_unit
        , sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0)) * any_value(coalesce(ingredient_prices.ingredient_actual_cost_per_unit, 0)) as total_ingredient_actual_cost
        , ceiling(sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0))) * any_value(coalesce(ingredient_prices.ingredient_actual_cost_per_unit, 0)) as total_ingredient_actual_cost_whole_units
        
        , any_value(recipe_ingredients.ingredient_co2_emissions_per_unit) as ingredient_co2_emissions_per_unit
        , sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0))  * any_value(recipe_ingredients.ingredient_co2_emissions_per_unit) as total_ingredient_co2_emissions
        , ceiling(sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0)))  * any_value(recipe_ingredients.ingredient_co2_emissions_per_unit) as total_ingredient_co2_emissions_whole_units
        , any_value(
              case
                  when recipe_ingredients.has_co2_data then recipe_ingredients.ingredient_net_weight
                  else 0
              end
          ) as ingredient_weight_with_co2_data
        , sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0)) * 
          any_value(
              case
                  when recipe_ingredients.has_co2_data then recipe_ingredients.ingredient_net_weight
                  else 0
              end
          ) as total_ingredient_weight_with_co2_data
        , ceiling(sum(coalesce(recipe_ingredients.recipe_ingredient_quantity, 0))) * 
          any_value(
              case
                  when recipe_ingredients.has_co2_data then recipe_ingredients.ingredient_net_weight
                  else 0
              end
          ) as total_ingredient_weight_with_co2_data_whole_units
    
    from menu_weeks_recipes_portions

    left join recipes
        on menu_weeks_recipes_portions.recipe_id = recipes.recipe_id

    left join recipe_ingredients
        on menu_weeks_recipes_portions.recipe_id = recipe_ingredients.recipe_id
        and menu_weeks_recipes_portions.portion_id_menus = recipe_ingredients.portion_id
        and menu_weeks_recipes_portions.language_id = recipe_ingredients.language_id

    left join ingredient_prices
        on menu_weeks_recipes_portions.menu_week_monday_date = ingredient_prices.menu_week_monday_date
        and menu_weeks_recipes_portions.company_id = ingredient_prices.company_id
        and recipe_ingredients.ingredient_id = ingredient_prices.ingredient_id

    where menu_weeks_recipes_portions.recipe_id is not null
        and recipe_ingredients.ingredient_id is not null
    
    group by all

)

select * from all_tables_joined