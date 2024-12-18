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
        , created_by as main_recipe_created_by
        , created_at as main_recipe_created_at
        , duplicated_by as source_created_by
        , duplicated_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

        {# strings #}
        , right(concat(00,recipe_variation_suffix),2) as main_recipe_variation_suffix
        , concat(main_recipe_id,right(concat(00,recipe_variation_suffix),2)) as main_recipe_variation_id

        {# numerics #}
        , cast(rating_average as decimal(6,4)) as recipe_average_rating
        , shelf_life as recipe_shelf_life_days

        {# booleans #}
        , recipe_universe as is_in_recipe_universe

        {# not sure if these are needed
        , photo_state
        , ingredients_state
        , edited_state
        , translations_state
        , like_counter
        , dislike_counter
        , text_editor
        , tested_state
        , extra_photo_state
        , recipe_hygiene_tip
        , created_by_override
        , is_active
        , tips_state
        #}

    from source

)

select * from renamed