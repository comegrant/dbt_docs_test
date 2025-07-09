with 

source as (

    select * from {{ source('partnership', 'partnership__company_partnership_rule') }}

)

, renamed as (

    select

        {# ids #}
        id as company_partnership_rule_id
        , company_partnership_id
        , partnership_rule_id
        
        {# booleans #}
        , is_active
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by


    from source

)

select * from renamed
