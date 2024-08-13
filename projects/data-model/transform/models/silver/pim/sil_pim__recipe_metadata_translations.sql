with 

source as (

    select * from {{ source('pim', 'pim_recipe_metadata_translations') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_metadata_id
        , language_id

        {# strings #}
        , initcap(recipe_name) as recipe_name
        , initcap(recipe_photo_caption) as recipe_photo_caption
        , initcap(roede_calculation_text) as roede_calculation_text
        , initcap(recipe_extra_photo_caption) as recipe_extra_photo_caption
        , initcap(general_text) as recipe_general_text
        , initcap(recipe_description) as recipe_description
        
    from source

)

select * from renamed