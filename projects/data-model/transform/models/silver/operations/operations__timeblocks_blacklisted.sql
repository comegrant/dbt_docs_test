with 

source as (

    select * from {{ source('operations', 'operations__timeblock_blacklist') }}

)

, renamed as (

    select 
        {# ids #}
        id as timeblocks_blacklisted_id
        , company_id

        {# ints #}
        , year as menu_year
        , week as menu_week
        , timeblock_id
        , fallback_timeblock_id

    from source

)

select * from renamed
