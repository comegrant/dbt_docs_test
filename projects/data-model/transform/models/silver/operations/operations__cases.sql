with 

source as (

    select * from {{ source('operations', 'operations__cases') }}

)

, renamed as (

    select
        {# ids #}
        case_id 
        , order_id as ops_order_id
        , case_re_delivery_timeblock_id as case_redelivery_timeblock_id
        , case_status as case_status_id
        , case_re_delivery as case_redelivery_id
        
        {# strings #}
        , case_re_delivery_comment  as case_redelivery_comment

        {# system #}
        , case_re_delivery_user  as source_updated_by
        , case_status_last_change as source_updated_at
        , case_re_delivery_date as source_created_at
        
    from source

)

select * from renamed