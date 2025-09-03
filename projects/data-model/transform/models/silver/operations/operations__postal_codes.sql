with 

source as (

    select * from {{ source('operations', 'operations__postal_codes') }}

)

, renamed as (

    select 
        {# ints #}
        postalcode_id as postal_code_id

        {# ids #}
        , country_id

        {# strings #}
        , {{ clean_postal_code('postalcode_id', 'country_id') }} as postal_code
        , city as city_name
        , county as county_name
        , municipality as municipality_name

        {# booleans #}
        , is_active
        , has_geofence

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
