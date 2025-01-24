with 

source as (

   select * from {{ source('analyticsdb', 'analyticsdb_orders__historical_orders_combined') }}

)

, renamed (

   select 

        {# ids #}
        order_id as ops_order_id
        , order_type_id
        , order_status_id
        , agreement_id as billing_agreement_id

        {# date #}
        , delivery_date

        {# timestamps #}
        , order_creation_date as source_created_at

   from source

)

select * from renamed