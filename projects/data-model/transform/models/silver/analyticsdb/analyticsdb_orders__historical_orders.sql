with 

source as (

   select * from {{ source('analyticsdb', 'analyticsdb_orders__historical_orders_combined') }}

)

, renamed (

   select 

        {# ids #}
        md5(order_id) as billing_agreement_order_id
        , order_id as ops_order_id
        , order_type_id
        , order_status_id
        , agreement_id as billing_agreement_id

        {# numerics #}
        , delivery_year as menu_year
        , delivery_week as menu_week

        , {{ get_iso_week_start_date('delivery_year', 'delivery_week') }} as menu_week_monday_date

        {# timestamps #}
        , order_creation_date as source_created_at

   from source

)
   

select * from renamed