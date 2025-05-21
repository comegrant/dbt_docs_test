with 

timeblocks_blacklisted as (

    select * from {{ ref('operations__timeblocks_blacklisted') }}

)

, timeblocks_blacklisted_zones as (

    select * from {{ ref('operations__timeblocks_blacklisted_zones') }}

)

, latest_menu_week as (

    select * from {{ ref('int_latest_menu_week_passed_cutoff') }}

)

, upcoming_blacklisted_timeblocks_and_zones_joined as (

    select 
        timeblocks_blacklisted.*
        , timeblocks_blacklisted_zones.zone_id
        , timeblocks_blacklisted.menu_year*100 + timeblocks_blacklisted.menu_week as menu_year_week

    from timeblocks_blacklisted
    left join timeblocks_blacklisted_zones 
        on timeblocks_blacklisted.timeblocks_blacklisted_id = timeblocks_blacklisted_zones.timeblocks_blacklisted_id
    left join latest_menu_week
        on timeblocks_blacklisted.company_id = latest_menu_week.company_id
        and timeblocks_blacklisted.menu_year*100 + timeblocks_blacklisted.menu_week > latest_menu_week.menu_year_week
)

, zones as (

    select * from {{ ref('operations__zones') }}

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, zones_postal_codes as (

    select * from {{ ref('operations__zones_postal_codes') }}

)

, postal_codes as (

    select * from {{ ref('operations__postal_codes') }}

)

, all_tables_joined as (

    select
        upcoming_blacklisted_timeblocks_and_zones_joined.timeblocks_blacklisted_id
        , upcoming_blacklisted_timeblocks_and_zones_joined.timeblock_id 
        , upcoming_blacklisted_timeblocks_and_zones_joined.menu_year
        , upcoming_blacklisted_timeblocks_and_zones_joined.menu_week
        , upcoming_blacklisted_timeblocks_and_zones_joined.company_id
        , upcoming_blacklisted_timeblocks_and_zones_joined.fallback_timeblock_id
        , zones.zone_id
        , zones.menu_year_week_from
        , zones.menu_year_week_to
        , zones.is_active as zone_is_active
        , zones_postal_codes.postal_code_id
        , postal_codes.postal_code

    from upcoming_blacklisted_timeblocks_and_zones_joined
    left join zones
        on upcoming_blacklisted_timeblocks_and_zones_joined.zone_id = zones.zone_id
        -- zone is both valid and active  
        and upcoming_blacklisted_timeblocks_and_zones_joined.menu_year_week >= zones.menu_year_week_from
        and upcoming_blacklisted_timeblocks_and_zones_joined.menu_year_week <= zones.menu_year_week_to
        and zones.is_active = 1
    left join companies
        on zones.company_id = companies.company_id
    left join zones_postal_codes
        on zones.zone_id = zones_postal_codes.zone_id
        and companies.country_id = zones_postal_codes.country_id
    left join postal_codes
        on zones_postal_codes.postal_code_id = postal_codes.postal_code_id
        and companies.country_id = postal_codes.country_id
    where zones.zone_id is not null -- if a zone was not valid over the blacklisted menu_year_week, exclude it 
    and zones_postal_codes.zone_id is not null -- if a zone-postcode relationship does not exist, exclude it

)

select * from all_tables_joined