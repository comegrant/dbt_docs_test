with 

source as (

    select * from {{ source('operations', 'operations__timeblock') }}

)

, renamed as (

    select

        {# ids #}
        cast(timeblock_id as int) as timeblock_id
        , country_id
        , timeblock_type_id

        {# strings #}
        , date_format(from, 'HH:mm') as time_from_default
        , date_format(to, 'HH:mm') as time_to_default

        {# numerics #}
        , case when weekday = 0 then 7 else weekday end as day_of_week_default
        , hour(from) * 100 + minute(from) as time_from_default_numeric
        , hour(to) * 100 + minute(to) as time_to_default_numeric

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

    from source

)

select * from renamed
