with 

source as (

    select * from {{ source('pim', 'pim_recipe_main_ingredients') }}

),

renamed as (

    select

      {# ids #}  
      recipe_main_ingredient_id
      , status_code_id as recipe_main_ingredient_status_code_id

      {# system #}
      , created_by as system_created_by
      , created_date as system_created_date
      , modified_by as system_modified_by
      , modified_date as system_modified_date

    from source

)

select * from renamed