/*This intermediate table returns a mapping between each unique ingredient combination used in recipes, and the ingredients. 
This forms the basis of dim_ingredient_combinations and bridge_ingredient_combinations_ingredients.*/

with

ingredient_combinations as (

    select * from {{ ref('int_recipes_with_ingredient_combinations') }}

)

, unique_combinations as (
    select distinct
        ingredient_combination_id
        , language_id
        , ingredient_id_list
    from ingredient_combinations
)

, ingredients_exploded as (
    select
        ingredient_combination_id
        , language_id
        , ingredient_id
    from unique_combinations
        lateral view explode(ingredient_id_list) as ingredient_id
    where ingredient_id is not null
)

select * from ingredients_exploded