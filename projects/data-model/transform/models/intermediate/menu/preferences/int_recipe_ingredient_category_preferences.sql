with

recipe_ingredients as (

    select * from {{ ref('int_recipe_ingredients_joined') }}

)

, category_hierarchy as (

    select * from {{ ref('int_ingredient_category_hierarchies') }}

)

, category_preference as (

    select * from {{ ref('pim__ingredient_category_preferences') }}

)

, preferences as (

    select * from {{ ref('cms__preferences') }}

)

, recipe_category_preference as (
    select distinct
        recipe_ingredients.recipe_id
        , category_preference.preference_id
    from recipe_ingredients
    left join category_hierarchy
        on recipe_ingredients.ingredient_id = category_hierarchy.ingredient_id
    left join category_preference
        on category_hierarchy.ingredient_category_id = category_preference.ingredient_category_id
    left join preferences
        on category_preference.preference_id = preferences.preference_id
    where
        category_preference.preference_id is not null
        and preferences.preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB' --only taste preferences
)


select * from recipe_category_preference
