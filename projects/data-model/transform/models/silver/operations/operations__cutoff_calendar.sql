with 

source as (

    select * from {{ source('operations', 'operations__cutoff_calendar') }}

)

, renamed as (

    select

        {# ids #}
        id as cutoff_calendar_id
        , cutoff_id

        {# strings #}
        , year_nr as menu_year
        , week_nr as menu_week

        {# timestamp #}
        , cutoff_time as cutoff_at_local_time
        , cutoff_utc as cutoff_at_utc
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed