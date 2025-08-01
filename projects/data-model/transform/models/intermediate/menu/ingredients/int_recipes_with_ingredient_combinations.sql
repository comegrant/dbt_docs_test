/* This intermediate table returns a mapping of recipes to their ingredient combinations.
Each recipe portion has a ingredient_combination_id, which will be used to connect the facts to dim_ingredient_combinations. */

with 

recipe_ingredients as (

    select * from {{ ref('int_recipe_ingredients_joined') }}

)

, distinct_ingredients_per_recipe_portion as (

    select distinct
    recipe_id
    , portion_id
    , ingredient_id
    , language_id
    from recipe_ingredients

)

, ingredients_list as (
    select 
    recipe_id
    , portion_id
    , language_id
    , array_sort(collect_list(ingredient_id)) as ingredient_id_list
    from distinct_ingredients_per_recipe_portion
    group by all
)

, add_combination_id as (
    select 
    *
    , md5(array_join(ingredient_id_list, ',')) as ingredient_combination_id
    from ingredients_list
)

select * from add_combination_id

