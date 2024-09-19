with 

source as (

    select * from {{ source('pim', 'pim__recipes') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_id
        , recipe_metadata_id
        , main_recipe_id
        , status_code_id as recipe_status_code_id

        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

        {# not sure if these are needed
        , photo_state
        , ingredients_state
        , edited_state
        , translations_state
        , recipe_variation_suffix
        , like_counter
        , dislike_counter
        , rating_average
        , text_editor
        , tested_state
        , extra_photo_state
        , duplicated_by
        , duplicated_at
        , recipe_hygiene_tip
        , created_by_override
        , is_active
        , recipe_id_label
        , tips_state
        , shelf_life
        , recipe_universe
        #}

    from source

)

select * from renamed