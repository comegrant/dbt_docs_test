with 

addresses as (

    select * from {{ ref('cms__addresses') }}

)

, add_pk as (
    select 
        md5(shipping_address_id) as pk_dim_addresses
        , shipping_address_id
        , postal_code
        , is_geo_restricted

    from addresses
)

select * from add_pk