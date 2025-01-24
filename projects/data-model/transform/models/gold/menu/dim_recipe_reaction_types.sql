with 

recipe_favorite_types as (

    select * from {{ ref('pim__recipe_favorite_types') }}

)

, select_columns as (
    select
        md5(cast(recipe_favorite_type_id as string)) as pk_dim_recipe_reaction_types
        , recipe_favorite_type_id as recipe_reaction_type_id
        , recipe_favorite_type_name as recipe_reaction_type_name
    from recipe_favorite_types
)

select * from select_columns