with

products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, rename_financial_product_type_name as (

    select
        product_concept_id
        , product_type_id
        , product_id
        , product_status_id
        , product_variation_id
        , company_id
        -- replace 'Customer Composed Mealbox' with 'Mealbox'
        , case 
            when product_concept_id = 'F735584A-666E-41BD-8F4D-4D80FBEDF49F'
            then 'Mealbox'
            else product_concept_name
        end as product_concept_name
        , product_name
        , product_status_name
        , product_variation_name
        , sku
        , meals
        , portions
        , portion_name
        , is_active_product
        , vat_rate
        , picking_line_label
        , maximum_order_quantity
        , sent_to_frontend
        , case
            when product_type_id = '{{ var("financial_product_type_id") }}' then true
            else false
        end as is_financial
        , case
            when product_type_id = '{{ var("financial_product_type_id") }}' then "Mealbox"
            else product_type_name
        end as product_type_name
        , has_extra_protein
    from products

    )

, add_unknown_row as (

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
        , is_active_product
        , vat_rate
        , picking_line_label
        , maximum_order_quantity
        , is_financial
        , sent_to_frontend
        , has_extra_protein

    from rename_financial_product_type_name

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
        , null              as is_active_product
        , null              as vat_rate
        , null              as picking_line_label
        , null              as maximum_order_quantity 
        , false             as is_financial
        , false             as sent_to_frontend
        , false             as has_extra_protein

)

select * from add_unknown_row
