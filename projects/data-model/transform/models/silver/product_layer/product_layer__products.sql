with 

source as (

    select * from {{ source('product_layer', 'product_layer__product') }}

),

renamed as (

    select
    
        {# ids #}
        id as product_id
        , product_type_id
        , status as product_status_id

        {# strings #}
        , initcap(name) as product_name
        , description as product_description

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed