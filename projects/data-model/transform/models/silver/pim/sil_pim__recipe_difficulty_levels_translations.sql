with 

source as (

    select * from {{ source('pim', 'pim_recipe_difficulty_levels_translations') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_difficulty_level_id
        , language_id

        {# strings #}
        , recipe_difficulty_name
        , recipe_difficulty_description
        
    from source

)

select * from renamed