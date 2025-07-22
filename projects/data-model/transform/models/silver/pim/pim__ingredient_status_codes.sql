with source as (

    select * from {{ source('pim', 'pim__ingredients_status_codes') }}

),

renamed as (

    select
        {# ids #} 
        id as ingredient_status_code_id
        , name as ingredient_status_name
        
        {# system #}    
        , created_by as source_created_by
        , created_at as source_created_at
        , update_by as source_updated_by
        , update_at as source_updated_at

    from source

)

select * from renamed