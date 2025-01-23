with

recipes as (

    select * from {{ ref('pim__recipes') }}

)

, taxonomy_preferences as (

    select * from {{ ref('int_recipe_taxonomy_preferences') }}

)

, category_preferences as (

    select * from {{ ref('int_recipe_ingredient_category_preferences') }}

)

, allergy_preferences as (

    select * from {{ ref('int_recipe_ingredient_allergy_preferences') }}

)

, unioned as (
    select
        recipes.recipe_id
        , taxonomy_preferences.preference_id
    from recipes
    left join taxonomy_preferences
        on recipes.recipe_id = taxonomy_preferences.recipe_id

    union all

    select
        recipe_id
        , preference_id
    from category_preferences

    union all

    select distinct
        recipe_id
        , preference_id
    from allergy_preferences
)

, recipe_preferences_list as (
    select
        recipe_id
        , array_sort(
            array_distinct(
                filter(collect_list(preference_id), x -> x is not null)
            )
        ) as preference_id_list
    from unioned
    group by 1
)

, preference_combination_id_added as (
    select
        recipe_id
        , md5(array_join(preference_id_list, ',')) as preference_combination_id
        , preference_id_list
    from recipe_preferences_list
)

select * from preference_combination_id_added
