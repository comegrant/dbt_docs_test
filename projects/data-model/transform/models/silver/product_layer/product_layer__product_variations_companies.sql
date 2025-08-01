with 

source as (

    select * from {{ source('product_layer', 'product_layer__product_variation_company') }}

),

renamed as (

    select
    
        {# ids #}
        variation_id as product_variation_id
        , company_id

        {# strings #}
        , initcap(name) as product_variation_name
        , description as product_variation_description

        {# booleans #}
        , send_frontend as sent_to_frontend

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed