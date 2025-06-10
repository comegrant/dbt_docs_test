{{
    config(
        materialized='incremental',
        unique_key='case_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('operations', 'operations__cases') }}

)

, renamed as (

    select
        {# ids #}
        case_id 
        , cast(order_id as bigint) as ops_order_id
        , cast(case_re_delivery_timeblock_id as int) as redelivery_timeblock_id
        , case_status as case_status_id
        , case_re_delivery as redelivery_status_id
        
        {# strings #}
        , case_re_delivery_comment  as redelivery_comment
        , case_re_delivery_user  as redelivery_user

        {# system #}
        , case_re_delivery_date as redelivery_at
        , case_status_last_change as source_updated_at
        
    from source

)

select * from renamed