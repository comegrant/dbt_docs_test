with

recipe_ingredients as (
    select * from {{ ref("recipe_ingredients") }}
)

, recipe_generic_ingredients_distinct as (
    select distinct
        recipe_portion_id
        , language_id
        , generic_ingredient_id
        , generic_ingredient_name
    from recipe_ingredients
)

, recipe_generic_ingredients_aggregated as (
    select
        recipe_portion_id
        , language_id
        , count(distinct generic_ingredient_id) as number_generic_ingredients
        , concat_ws(
            ",", collect_list(generic_ingredient_id)
        )                                       as generic_ingredient_id_list
        , concat_ws(
            ", ", collect_list(generic_ingredient_name)
        )                                       as generic_ingredient_name_list
    from recipe_generic_ingredients_distinct
    group by 1, 2
)

, recipe_ingredients_aggregated as (
    select
        recipe_portion_id
        , language_id
        , concat_ws(", ", collect_list(ingredient_id)) as ingredient_id_list
        , concat_ws(
            ", ", collect_list(ingredient_category_id)
        )                                              as ingredient_category_id_list
    from recipe_ingredients
    group by 1, 2
)

, recipe_generic_ingredients_aggregated_joined as (
    select
        recipe_generic_ingredients_aggregated.*
        , recipe_ingredients_aggregated.ingredient_id_list
        , recipe_ingredients_aggregated.ingredient_category_id_list
    from recipe_generic_ingredients_aggregated
    left join recipe_ingredients_aggregated
        on
            recipe_generic_ingredients_aggregated.recipe_portion_id
            = recipe_ingredients_aggregated.recipe_portion_id
            and recipe_generic_ingredients_aggregated.language_id
            = recipe_ingredients_aggregated.language_id
)

select * from recipe_generic_ingredients_aggregated_joined
