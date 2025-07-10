with 

orders as (

    select * from {{ ref('operations__orders') }}

)

, zones_postal_codes as (

    select * from {{ ref('operations__zones_postal_codes') }}

)

, zones as (

    select * from {{ ref('operations__zones') }}

)

, tables_joined as (

    select 
        orders.* except(zone_id)
        , coalesce(orders.zone_id, zones_postal_codes.zone_id) as zone_id
    from orders
    left join zones_postal_codes
        on orders.postal_code_id = zones_postal_codes.postal_code_id
        and orders.country_id >= zones_postal_codes.country_id
    left join zones
        on zones_postal_codes.zone_id = zones.zone_id 
        and orders.company_id = zones.company_id 
        and orders.timeblock_id = zones.timeblock_id 
        and orders.menu_year_week >= zones.menu_year_week_from 
        -- TODO: In ADB we use =< but I think that seem strange?
        and orders.menu_year_week < zones.menu_year_week_to
        -- TODO: A bit unsure about why this is needed?
        and zones.is_active = 1 

)

select * from tables_joined