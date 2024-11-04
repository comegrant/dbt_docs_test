with 

source as (

   select * from {{ ref('scd_cms__billing_agreement_basket_products') }}

)

, renamed as (

    select
        
        {# ids #}
        id as billing_agreement_basket_product_id
        , billing_agreement_basket_id
        , subscribed_product_variation_id as product_variation_id
        , delivery_week_type as product_delivery_week_type_id

        {# numerics #}
        , quantity as product_variation_quantity
        
        {# booleans #}
        , is_extra as is_extra_product
        
        {# scd #}
        , convert_timezone('Europe/Oslo', 'UTC', dbt_valid_from) as valid_from
        , convert_timezone('Europe/Oslo', 'UTC', dbt_valid_to) as valid_to
        
        {# system #}
        , convert_timezone('Europe/Oslo', 'UTC', created_at) as source_created_at
        , created_by as source_created_by
        , convert_timezone('Europe/Oslo', 'UTC', updated_at) as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
