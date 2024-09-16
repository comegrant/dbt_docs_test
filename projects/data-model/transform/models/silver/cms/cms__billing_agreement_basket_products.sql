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
        , delivery_week_type as delivery_week_type_id

        {# numerics #}
        , quantity as product_variation_quantity
        
        {# booleans #}
        , is_extra
        
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
