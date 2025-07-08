with 

source as (

    select * from {{ source('operations', 'operations__transport_company') }}

)

, renamed as (

    select
        {# ids #}
        cast(transport_company_id as int) as transport_company_id
        
        {# strings #}
        , transport_name as transport_company_name

        {# system #}
        , created_date as source_created_at
        , created_by as source_created_by
        , modified_date as source_updated_at
        , modified_by as source_updated_by
        
    from source

)

select * from renamed