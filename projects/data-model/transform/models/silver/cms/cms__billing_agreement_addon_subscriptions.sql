with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_addon_subscriptions') }}

),

renamed as (

    select
        {# ids #}
        agreement_id as billing_agreement_id
        , addon_subscriptions_id as addon_subscription_id
        , company_id

        {# boolean #}
        , is_active

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
        
    from source

)

select * from renamed