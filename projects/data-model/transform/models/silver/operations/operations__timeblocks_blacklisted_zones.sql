with 

source as (

    select * from {{ source('operations', 'operations__zones_blacklist') }}

)

, renamed as (

    select 
        {# ids #}
        timeblock_blacklist_id as timeblocks_blacklisted_id

        {# ints #}
        , zone_id

    from source

)

select * from renamed
