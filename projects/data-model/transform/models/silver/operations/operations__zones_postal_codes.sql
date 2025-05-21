with 

source as (

    select * from {{ source('operations', 'operations__zone_postcode') }}

)

, renamed as (

    select 
        {# ints #}
        zone_id
        , postalcode_id as postal_code_id

        {# ids #}
        , country_id

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by

    from source

)

select * from renamed
