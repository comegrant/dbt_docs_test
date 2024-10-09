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
        , delivery_week_type as basket_delivery_week_type_id
        , timeblock as timeblock_id
        
        {# booleans #}
        , is_default as is_default_basket
        , is_active as is_active_basket
        
        {# scd #}
        , dbt_valid_from as valid_from
        , {{ get_scd_valid_to('dbt_valid_from', 'id') }} as valid_to

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
