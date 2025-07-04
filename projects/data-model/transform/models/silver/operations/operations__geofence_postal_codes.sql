with 

source as (

    select * from {{ ref('scd_operations__geofence_postal_codes') }}

)

, renamed as (
    select 
        {# ids #}
        id as geofence_postal_code_id
        , geofence_id
        , country_id
        , postal_code_id

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