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
        , zones_postal_codes.zone_id as postal_zone_id
        , orders.zone_id as orders_zone_id
        , zones.zone_id as zones_zone_id
        , coalesce(orders.zone_id, zones.zone_id) as zone_id
    from orders
    -- find the zones relating to the postal code
    -- one postal code can have several zones, so this join leads to duplicate rows
    left join zones_postal_codes
        on orders.postal_code_id = zones_postal_codes.postal_code_id
        and orders.country_id = zones_postal_codes.country_id
        -- only find zones for orders that are not in the orders history table yet 
        and orders.is_history_base is false
    -- find the zone relating to the order
    left join zones
        on zones_postal_codes.zone_id = zones.zone_id 
        and orders.company_id = zones.company_id 
        and orders.timeblock_id = zones.timeblock_id 
        and orders.menu_year_week >= zones.menu_year_week_from 
        and orders.menu_year_week <= zones.menu_year_week_to
        and zones.is_active = true
    -- remove duplicates that occurs due to the join with zones_postal_codes
    where not (zones.zone_id is null and orders.is_history_base is false)

)

select * from tables_joined