with 

source as (

    select * from {{ source('product_layer', 'product_layer__product_variation_attribute_value') }}

),

renamed as (

    select
    
        {# ids #}
        attribute_id
        , variation_id as product_variation_id
        , company_id
        
        {# strings #}
        , attribute_value
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed