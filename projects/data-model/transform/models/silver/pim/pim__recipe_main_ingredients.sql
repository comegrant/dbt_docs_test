with 

source as (

    select * from {{ source('pim', 'pim__recipe_main_ingredients') }}

),

renamed as (

    select

      {# ids #}  
      recipe_main_ingredient_id
      , status_code_id as recipe_main_ingredient_status_code_id

      {# system #}
      , created_by as source_created_by
      , created_date as source_created_at
      , modified_by as source_updated_by
      , modified_date as source_updated_at

    from source

)

select * from renamed