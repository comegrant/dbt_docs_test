with 

source as (

    select * from {{ source('cms', 'cms__loyalty_event') }}

)

, renamed as (

    select

        {# ids #}
        id as loyalty_event_id

        {# strings #}
        , initcap(name) as loyalty_event_name
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

, add_no_orders_previous_12_weeks_point_deduction_event as (

    select 

        {# ids #}
        '10000000-0000-0000-0000-000000000000' as loyalty_event_id

        {# strings #}
        , 'No orders placed in last 12 weeks' as loyalty_event_name

        {# system #}
        , cast('2025-01-16 15:00:00' as timestamp) as source_created_at
        , 'Analytics' as source_created_by
        , cast('2025-01-16 15:00:00' as timestamp) as source_updated_at
        , 'Analytics' as source_updated_by

)

, add_loyalty_order_event as (

    select 

        {# ids #}
        '20000000-0000-0000-0000-000000000000' as loyalty_event_id

        {# strings #}
        , 'Loyalty Order Event' as loyalty_event_name

        {# system #}
        , cast('2025-03-11 15:00:00' as timestamp) as source_created_at
        , 'Analytics' as source_created_by
        , cast('2025-03-11 15:00:00' as timestamp) as source_updated_at
        , 'Analytics' as source_updated_by

)

, unioned as (

    select * from renamed 

    union all 

    select * from add_no_orders_previous_12_weeks_point_deduction_event

    union all 

    select * from add_loyalty_order_event

)

select * from unioned
