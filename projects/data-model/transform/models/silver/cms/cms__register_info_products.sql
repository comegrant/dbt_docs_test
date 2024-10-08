with 

source as (

    select * from {{ source('cms', 'cms__register_info_products') }}

)

, renamed as (

    select

        
        {# ids #}
        id as register_info_product_id
        , register_id as register_info_basket_id
        , upper(menu_variation) as product_variation_id

        {# numerics #}
        , coalesce(menu_variation_quantity,0) as product_variation_quantity

    from source

)

select * from renamed
