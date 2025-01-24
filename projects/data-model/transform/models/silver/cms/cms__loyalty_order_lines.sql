with 

source as (

    select * from {{ source('cms', 'cms__loyalty_order_line') }}

),

renamed as (

    select
        
        {# ids #}
        id as loyalty_order_line_id
        , loyalty_order_id
        , variation_id as product_variation_id
        
        {# ints #}
        , points as unit_point_price
        , quantity as product_variation_quantity
        , points * quantity as total_point_price
        
        {# timestamps #}
        , created_at as source_created_at
        , updated_at as source_updated_at

        {# strings #}
        , created_by as source_created_by
        , updated_by as source_updated_by

    from source

)

select * from renamed