with 

source as (

    select * from {{ source('finance', 'finance__invoice_process_status') }}

)

, renamed as (

    select

        
        {# ids #}
        id as invoice_process_status_id

        {# strings #}
        , initcap(name) as invoice_process_status_name

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
