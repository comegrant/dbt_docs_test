with 

source as (

    select * from {{ source('pim', 'pim__weekly_orders') }}

)

, renamed as (

    select
        
        {# ids #}
        id as purchase_order_id
        , supplier_id as ingredient_supplier_id
        , company_id as purchasing_company_id
        , status as purchase_order_status_id
        , delivery_distribution_center_id as distribution_center_id


        {# numerics #}
        , week as menu_week
        , year as menu_year
        
        {# booleans #}
        , from_purchase_order as is_special_purchase_order
        
        {# date #}
        , production_date
        , {{ get_iso_week_start_date('year', 'week') }} as menu_week_monday_date
        
        {# timestamp #}
        , delivery_date as purchase_delivery_date
        , order_date as purchase_order_date
        
        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed




