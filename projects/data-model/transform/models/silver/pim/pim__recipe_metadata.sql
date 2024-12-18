with

source as (

    select * from {{ source('pim', 'pim__recipes_metadata') }}

)

, renamed as (

    select

    {# ids #}
        recipe_metadata_id
        , recipe_main_ingredient_id
        , recipe_difficulty_level_id

        {# ints #}
        , cooking_time_from
        , cooking_time_to

        {# strings #}
        , recipe_photo
        , concat(cooking_time_from, '-', cooking_time_to) as cooking_time

        {# booleans #}
        , coalesce(recipe_photo is not null, false)      as has_recipe_photo

        {# system #}
        , created_by                                      as source_created_by
        , created_date                                    as source_created_at
        , modified_by                                     as source_updated_by
        , modified_date                                   as source_updated_at

    {# not needed?
      , recipe_photo_resized
      , recipe_photo_large_resized
      , recipe_extra_photo
      , recipe_extra_photo_resized
      , recipe_extra_photo_large_resized
    #}

    from source

)

select * from renamed
