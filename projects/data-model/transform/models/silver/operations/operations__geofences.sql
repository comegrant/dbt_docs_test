with 

source as (

    select * from {{ ref('scd_operations__geofences') }}

)

, renamed as (

    select 

        {# ids #}
        id as geofence_id
        , country_id
        , layer_id as geofence_layer_id
        , container_id as geofence_container_id
        , polygon_id as geofence_polygon_id

        {# strings #}
        , layer_name as geofence_layer_name
        , container_name as geofence_container_name
        , polygon_name as geofence_polygon_name

        {# booleans #}
        , is_active

        {# scd #}
        , dbt_valid_from as valid_from
        , {{ get_scd_valid_to('dbt_valid_to') }} as valid_to

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed