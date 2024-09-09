with 

source as (

    select * from {{ source('cms', 'cms__loyalty_agreement_ledger') }}

),

renamed as (

    select
        
        {# ids #}
        id as loyalty_agreement_ledger_id
        , agreement_id as billing_agreement_id
        , event_id as loyalty_event_id
        , current_level as loyalty_level_id

        {# numerics #}
        , accrued_points

        {# timestamp #}
        , timestamp as points_generated_at
        
    from source

)

select * from renamed