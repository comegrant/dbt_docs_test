with 

source as (

    select * from {{ source('operations', 'operations__distribution_center') }}

)

, renamed as (

    select
        
        {# ids #}
        id as distribution_center_id
        , distribution_center_type as distribution_center_type_id
        , cast(transport_company_id as int) as transport_company_id
        , country_id
        
        {# strings #}
        , alias as distribution_center_name

        {# numbers #}
        , {{ clean_postal_code('postalcode', 'country_id') }} as postal_code

        , latitude as latitude
        , longitude as longitude

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
