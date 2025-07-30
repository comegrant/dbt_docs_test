with 

source as (

    select * from {{ source('partnership','partnership__company_partnership') }}

)

, renamed as (

    select

        {# ids #}
        id as company_partnership_id
        , company_id
        , partnership_id
        
        {# booleans #}
        , is_active as is_active_partnership
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
