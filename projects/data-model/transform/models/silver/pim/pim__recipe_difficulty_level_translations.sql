with 

source as (

    select * from {{ source('pim', 'pim__recipe_difficulty_levels_translations') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_difficulty_level_id
        , language_id

        {# strings #}
        , initcap(recipe_difficulty_name) as recipe_difficulty_name
        , initcap(recipe_difficulty_description) as recipe_difficulty_description
        
    from source

)

select * from renamed