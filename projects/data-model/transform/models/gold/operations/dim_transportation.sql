with 

zones as (

    select * from {{ ref('operations__zones') }}

)

, transport_companies as (

    select * from {{ ref('operations__transport_companies') }}

)

, distribution_centers as (

    select * from {{ ref('operations__distribution_centers') }}

)

, distribution_center_types as (

    select * from {{ ref('operations__distribution_center_types') }}

)

, join_tables as (

    select
        md5(cast(zones.zone_id as string)) as pk_dim_transportation
        , zones.zone_id
        , zones.transport_company_id
        , zones.last_mile_hub_distribution_center_id
        , zones.packing_distribution_center_id
        , transport_companies.transport_company_name
        , distribution_center_last_mile.distribution_center_name as last_mile_distribution_site
        , distribution_center_last_mile.postal_code as last_mile_distribution_postal_code
        , distribution_center_last_mile.longitude as last_mile_distribution_longitude
        , distribution_center_last_mile.latitude as last_mile_distribution_latitude
        , distribution_center_packing.distribution_center_name as production_site
    from zones
    left join transport_companies
        on zones.transport_company_id = transport_companies.transport_company_id
    left join distribution_centers as distribution_center_last_mile
        on zones.last_mile_hub_distribution_center_id = distribution_center_last_mile.distribution_center_id
    left join distribution_centers as distribution_center_packing
        on zones.packing_distribution_center_id = distribution_center_packing.distribution_center_id

)

, add_unknown_row as (

    select * from join_tables

    union all

    select
        '0'  as pk_dim_transportation
        , 0 as zone_id
        , 0 as company_id
        , '0' as last_mile_hub_distribution_center_id
        , '0' as packing_distribution_center_id
        ,'unknown' as transport_company_name
        , 'unknown' as last_mile_distribution_site
        , null as last_mile_distribution_postal_code
        , null as last_mile_distribution_longitude
        , null as last_mile_distribution_latitude
        , 'unknown' as production_site
)

select * from add_unknown_row
