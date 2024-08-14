with 

source as (

    select * from {{ source('cms', 'cms__addon_subscriptions') }}

),

renamed as (

    select
        
        {# ids #}
        id as addon_subscription_id
        , company_id
        , variation_id as product_variation_id

        {# strings #}
        , name as addon_subscription_name

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
        
    from source

)

select * from renamed