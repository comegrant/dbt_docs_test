with 

order_lines as (

    select * from {{ ref('cms__billing_agreement_order_lines')}}
    
)

, has_delivery (
    select distinct
        billing_agreement_order_id,
        true as has_delivery
    from order_lines
    {# Only include transportation lines #}
    where product_variation_id = 'DEAEF8CD-8D49-4689-84FE-CF6D1A43888A'
)

select * from has_delivery