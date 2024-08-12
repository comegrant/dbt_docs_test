with 

source as (

    select * from {{ source('product_layer', 'product_layer_product_type') }}

),

renamed as (

    select

        {# ids #}
        product_type_id

        {# strings #}
        , initcap(product_type_name) as product_type_name
        , product_type_description

        {# booleans #}
        , physical_product as is_physical_product

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed