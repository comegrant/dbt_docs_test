with 

source as (

    select * from {{ source('operations', 'operations__zones') }}

)

, renamed as (

    select 
        {# ints #}
        zone_id
        , timeblock_id
        , cast(transport_company_id as int) as transport_company_id
        , cast(period_from as int) as menu_year_week_from
        , cast(period_to as int) as menu_year_week_to

        {# ids #}
        , company_id
        , packing_distribution_center_id
        , last_mile_hub_distribution_center_id

        {# booleans #}
        , is_active

        {# system #}
        , created_date as source_created_at
        , created_by as source_created_by
        , modified_date as source_updated_at
        , modified_by as source_updated_by

    from source
)

select * from renamed
