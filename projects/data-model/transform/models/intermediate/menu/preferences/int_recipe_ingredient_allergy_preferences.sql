with

recipe_ingredients as (

    select * from {{ ref('int_recipe_ingredients_joined') }}

)

, ingredient_allergies as (

    select * from {{ ref('pim__ingredient_allergies') }}

)

, allergy_preference as (

    select * from {{ ref('pim__allergy_preferences') }}

)

, preferences as (

    select * from {{ ref('cms__preferences') }}

)

, recipe_allergy_preference as (
    select distinct
        recipe_ingredients.recipe_id
        , recipe_ingredients.ingredient_id
        , allergy_preference.preference_id
    from recipe_ingredients
    left join ingredient_allergies
        on recipe_ingredients.ingredient_id = ingredient_allergies.ingredient_id
    left join allergy_preference
        on ingredient_allergies.allergy_id = allergy_preference.allergy_id
    left join preferences
        on allergy_preference.preference_id = preferences.preference_id
    where
        allergy_preference.preference_id is not null
        and preferences.preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB' --only taste preferences
)

select * from recipe_allergy_preference
