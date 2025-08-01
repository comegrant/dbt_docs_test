with 

ingredient_combinations as (

    select * from {{ ref('int_ingredient_combinations') }}

)

, add_keys as (

    select 
    md5(concat_ws(
            '-'
            , ingredient_combination_id
            , ingredient_id
            , language_id
        )) as pk_bridge_ingredient_combinations_ingredients
    , ingredient_combination_id
    , language_id
    , ingredient_id
    , coalesce(
            md5(concat(ingredient_combination_id, language_id))
            , '0'
         ) as fk_dim_ingredient_combinations
    , md5(concat_ws(
            '-'
            , ingredient_id
            , language_id
        )) as fk_dim_ingredients
    from ingredient_combinations

)

select * from add_keys