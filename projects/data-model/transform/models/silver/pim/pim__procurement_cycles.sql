with 

source as (

    select * from {{ source('pim', 'pim__procurement_cycle') }}

)

, renamed as (

    select
        
        {# ids #}
        company_id
        , buy_as_company_id as purchasing_company_id
        , distribution_center_id
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
