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
        , orders.zone_id as orders_zone_id
        , zones_postal_codes.zone_id as postal_zone_id
        , zones.zone_id as zone_id
    from orders
    left join zones_postal_codes
        on orders.postal_code_id = zones_postal_codes.postal_code_id
        and orders.country_id = zones_postal_codes.country_id
        and orders.zone_id is null
    left join zones
        on zones_postal_codes.zone_id = zones.zone_id 
        and orders.company_id = zones.company_id 
        and orders.timeblock_id = zones.timeblock_id 
        and orders.menu_year_week >= zones.menu_year_week_from 
        -- TODO: In ADB we use =< but I think that seem strange? Have to ask OPS
        and orders.menu_year_week < zones.menu_year_week_to
        and zones.is_active = 1
    where zones.zone_id is not null or orders.zone_id is not null

)

select * from tables_joined