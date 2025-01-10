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

select * from renamed
