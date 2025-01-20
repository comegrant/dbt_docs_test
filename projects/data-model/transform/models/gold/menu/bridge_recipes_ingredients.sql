with

recipes as (

    select * from {{ ref('dim_recipes') }}

)

, ingredients as (

    select * from {{ ref('dim_ingredients') }}

)

, int_recipe_ingredients as (

    select distinct
        recipe_id
        , recipe_portion_id
        , ingredient_id
    from {{ ref('int_recipe_ingredients_joined') }}

)

, bridge_recipes_ingredients as (
    select
        md5(
            cast(concat(
                recipes.pk_dim_recipes
                , int_recipe_ingredients.recipe_portion_id
                , ingredients.pk_dim_ingredients
            ) as string)
        )                                as pk_bridge_recipes_ingredients
        , recipes.pk_dim_recipes         as fk_dim_recipes
        , ingredients.pk_dim_ingredients as fk_dim_ingredients
        , recipes.recipe_id
        , int_recipe_ingredients.recipe_portion_id
        , ingredients.language_id
        , ingredients.ingredient_id
        , ingredients.allergy_id
    from recipes
    left join int_recipe_ingredients
        on recipes.recipe_id = int_recipe_ingredients.recipe_id
    left join ingredients
        on int_recipe_ingredients.ingredient_id = ingredients.ingredient_id
    where ingredients.ingredient_id is not null
)

select * from bridge_recipes_ingredients
