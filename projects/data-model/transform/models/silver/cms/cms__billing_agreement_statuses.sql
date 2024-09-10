with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_status') }}

),

renamed as (

    select
        {# ids #}
        status_id as billing_agreement_status_id

        {# strings #}
        , initcap(status_description) as billing_agreement_status_name
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed