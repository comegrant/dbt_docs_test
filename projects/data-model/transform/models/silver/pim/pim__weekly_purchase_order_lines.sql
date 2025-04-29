with 

source as (

    select * from {{ source('pim', 'pim__weekly_orders_lines') }}

)

, renamed as (

    select
        
        {# ids #}
        id as purchase_order_line_id
        , weekly_order_id as purchase_order_id
        , ingredient_id
        , status as purchase_order_line_status_id

        {# numerics #}
        , ingredient_quantity as original_ingredient_quantity
        , ingredient_cost as ingredient_purchasing_cost
        , (cast(vat as decimal (6,4))/100) as vat
        , total_order_line_cost as total_purchasing_cost
        , ingredient_extra_quantity as extra_ingredient_quantity 
        , ingredient_total_quantity as total_ingredient_quantity
        , take_from_storage as take_from_storage_ingredient_quantity
        , received_quantity as received_ingredient_quantity
        
        {# timestamp #}
        , delivery_date as purchase_delivery_date
        
        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
