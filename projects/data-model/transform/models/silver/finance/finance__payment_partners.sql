with 

source as (

    select * from {{ source('finance', 'finance__payment_partner') }}

)

, renamed as (

    select

        
        {# ids #}
        id as payment_partner_id
        , company_id

        {# strings #}
        , name as payment_partner_name
        
        {# booleans #}
        , is_active as is_active_payment_partner
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
