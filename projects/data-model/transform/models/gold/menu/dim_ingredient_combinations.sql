with ingredient_combinations as (

    select * from {{ ref('int_ingredient_combinations') }}

)

, ingredients as (

    select * from {{ ref('dim_ingredients') }}

)

, add_ingredient_info as (
    select 
    ingredient_combinations.ingredient_combination_id
    , ingredient_combinations.language_id
    , ingredient_combinations.ingredient_id
    , ingredients.ingredient_internal_reference
    , ingredients.ingredient_name
    from ingredient_combinations
    left join ingredients
        on ingredient_combinations.ingredient_id = ingredients.ingredient_id
        and ingredient_combinations.language_id = ingredients.language_id
)

, collect_lists as (
    select
        ingredient_combination_id
        , language_id
        , array_sort(collect_list(ingredient_id)) as ingredient_id_list
        , array_sort(collect_list(ingredient_internal_reference)) as ingredient_internal_reference_list
        , array_sort(collect_list(ingredient_name)) as ingredient_name_list
    from add_ingredient_info
    group by all
)

, convert_to_strings as (
    select
        ingredient_combination_id
        , language_id
        , ingredient_id_list as ingredient_id_list_array
        , coalesce(
            nullif(array_join(ingredient_id_list, ', '),''),
            'No ingredients'
         ) as ingredient_id_combinations
        , coalesce(
            nullif(array_join(ingredient_internal_reference_list, ', '),''),
            'No ingredients'
         ) as ingredient_internal_reference_combinations
        , coalesce(
            nullif(array_join(ingredient_name_list, ', '),''),
            'No ingredients'
         ) as ingredient_name_combinations
    from collect_lists
)

, add_unknown_row as (

    select
        coalesce(
            md5(concat(ingredient_combination_id, language_id))
            , '0'
         ) as pk_dim_ingredient_combinations
        , *
    from convert_to_strings

    union all

    select
        '0'                 as pk_dim_ingredient_combinations
        , '0'               as ingredient_combination_id
        , 0                 as language_id
        , array()           as ingredient_id_list_array
        , 'No ingredients'  as ingredient_id_combinations
        , 'No ingredients'  as ingredient_internal_reference_combinations
        , 'No ingredients'  as ingredient_name_combinations

)

select * from add_unknown_row