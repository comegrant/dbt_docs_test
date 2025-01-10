with 

source as (

    select * from {{ source('analyticsdb', 'analyticsdb_snapshots__agreement_status') }}

)

, renamed as (

    select

        
        {# ids #}
        cast(agreement_id as int) as billing_agreement_id
        , cast(status as int) as billing_agreement_status_id
        
        {# system #}
        , snapshot_date as source_created_at

    from source

)

select * from renamed
