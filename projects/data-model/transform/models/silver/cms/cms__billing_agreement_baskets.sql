with 

source as (

   select * from {{ ref('scd_cms__billing_agreement_baskets') }}

)

, renamed as (

    select

        
        {# ids #}
        id as billing_agreement_basket_id
        , agreement_id as billing_agreement_id
        , shipping_address as shipping_address_id
        , delivery_week_type as delivery_week_type_id
        , timeblock as timeblock_id
        
        {# booleans #}
        , is_default
        , is_active
        
        {# scd #}
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
