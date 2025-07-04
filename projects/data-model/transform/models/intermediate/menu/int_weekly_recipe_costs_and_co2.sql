with

weekly_recipe_ingredients as (

    select * from {{ ref('int_weekly_recipe_ingredients') }}

)

, weekly_recipe_ingredients_aggregated as (

    select
        company_id
        , menu_year
        , menu_week
        , recipe_id
        , recipe_portion_id

        , sum(total_ingredient_weight)                              as total_ingredient_weight
        , sum(total_ingredient_weight_whole_units)                  as total_ingredient_weight_whole_units

        , sum(total_ingredient_planned_cost)                        as total_ingredient_planned_cost
        , sum(total_ingredient_planned_cost_whole_units)            as total_ingredient_planned_cost_whole_units

        , sum(total_ingredient_expected_cost)                       as total_ingredient_expected_cost
        , sum(total_ingredient_expected_cost_whole_units)           as total_ingredient_expected_cost_whole_units

        , sum(total_ingredient_actual_cost)                         as total_ingredient_actual_cost
        , sum(total_ingredient_actual_cost_whole_units)             as total_ingredient_actual_cost_whole_units

        , sum(total_ingredient_co2_emissions)                       as total_ingredient_co2_emissions
        , sum(total_ingredient_co2_emissions_whole_units)           as total_ingredient_co2_emissions_whole_units

        , sum(total_ingredient_weight_with_co2_data)                as total_ingredient_weight_with_co2_data
        , sum(total_ingredient_weight_with_co2_data_whole_units)    as total_ingredient_weight_with_co2_data_whole_units

    from weekly_recipe_ingredients

    group by all

)

select * from weekly_recipe_ingredients_aggregated
