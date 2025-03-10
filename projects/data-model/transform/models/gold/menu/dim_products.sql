with 

products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, add_unknown_row (

    select 
        md5(
            concat(
                product_variation_id,
                company_id)
            )
        as pk_dim_products
        , product_concept_id
        , product_type_id
        , product_id
        , product_status_id
        , product_variation_id
        , company_id
        , product_concept_name
        , product_type_name
        , product_name
        , product_status_name
        , product_variation_name
        , sku
        , meals
        , portions
        , portion_name
    from products

    union all

    select 
        '0'                 as pk_dim_products
        , '0'               as product_concept_id
        , '0'               as product_type_id
        , '0'               as product_id
        , 0                 as product_status_id
        , '0'               as product_variation_id
        , '0'               as company_id
        , "Not relevant"    as product_concept_name
        , "Not relevant"    as product_type_name
        , "Not relevant"    as product_name
        , "Not relevant"    as product_status_name
        , "Not relevant"    as product_variation_name
        , "0"               as sku
        , null              as meals
        , null              as portions
        , null              as portion_name

)

select * from add_unknown_row