with 

source as (

    select * from {{ source('operations', 'operations__timeblock_calendar') }}

)

, renamed as (

    select

        
        {# ids #}
         id as timeblock_calendar_id
        , timeblock_id

        {# strings #}
        , year_nr as menu_year
        , week_nr as menu_week
        , date_format(from_localtime, 'HH:mm') as time_from
        , date_format(to_localtime, 'HH:mm') as time_to

        {# numerics #}
        , dayofweek(delivery_date) as day_of_week
        , period_nr as menu_year_week
        , hour(from_localtime) * 100 + minute(from_localtime) as time_from_numeric
        , hour(to_localtime) * 100 + minute(to_localtime) as time_to_numeric
        , hour(from_localtime) * 1000 + minute(from_localtime) * 10 + dayofweek(delivery_date) as delivery_window_start_numeric

        
        {# date #}
        , {{ get_iso_week_start_date('year_nr', 'week_nr') }} as menu_week_monday_date
        , delivery_date as delivery_date
        
        {# timestamp #}
        , from_localtime as timeblock_started_at_local
        , from_utc as timeblock_started_at_utc
        , to_localtime as timeblock_ended_at_local
        , to_utc as timeblock_ended_at_utc
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by


    from source

)

select * from renamed
