with 

source as (

    select * from {{ source('product_layer', 'product_layer_product_variation_attribute_template') }}

),

renamed as (

    select
    
        {# ids #}
        attribute_id
        , product_type_id
        
        {# strings #}
        , attribute_name
        , default_value as attribute_default_value
        , data_type as attribute_data_type

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed