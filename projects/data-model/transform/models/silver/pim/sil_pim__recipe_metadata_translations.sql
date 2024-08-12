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
        , recipe_name
        , recipe_photo_caption
        , roede_calculation_text
        , recipe_extra_photo_caption
        , general_text as recipe_general_text
        , recipe_description
        
    from source

)

select * from renamed