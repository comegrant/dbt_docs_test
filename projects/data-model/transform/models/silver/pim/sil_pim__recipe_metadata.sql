with 

source as (

    select * from {{ source('pim', 'pim_recipes_metadata') }}

),

renamed as (

    select

      {# ids #}  
      recipe_metadata_id
      , recipe_main_ingredient_id
      , recipe_difficulty_level_id
      
      {# ints #}
      , cooking_time_from
      , cooking_time_to

      {# system #}
      , created_by as system_created_by
      , created_date as system_created_date
      , modified_by as system_modified_by
      , modified_date as system_modified_date

    {# not needed?
      , recipe_photo
      , recipe_photo_resized
      , recipe_photo_large_resized
      , recipe_extra_photo
      , recipe_extra_photo_resized
      , recipe_extra_photo_large_resized
    #}
        
    from source

)

select * from renamed