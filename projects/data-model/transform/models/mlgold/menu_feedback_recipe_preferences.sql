with

recipes as (

    select * from {{ ref('dim_recipes') }}

)

, preferences as (

    select * from {{ ref('int_recipe_preferences_unioned') }}

)

, preference_combinations as (

    select * from {{ ref('dim_all_preference_combinations') }}

)

, recipe_preferences as (

    select distinct
        recipes.main_recipe_id
        , recipes.recipe_main_ingredient_id
        --, recipes.recipe_main_ingredient_name_local
        --, recipes.language_id
        , preference_combinations.taste_name_combinations_including_allergens as negative_taste_preferences
    from recipes
    left join preferences
        on recipes.recipe_id = preferences.recipe_id
    left join preference_combinations
        on preferences.preference_combination_id = preference_combinations.pk_preference_combination_id
    where recipes.is_main_recipe = true

)

select * from recipe_preferences
