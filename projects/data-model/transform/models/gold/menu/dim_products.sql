with 

products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, mealbox_product_mapping as (

    select * from {{ ref('int_product_variations_financial_mealbox_product_mapping') }}

)

, add_preselected_products (
    select 
        products.*
        , case 
            when products.product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
            then products.product_id
            when products.product_type_id = '288ED8CA-B437-4A6D-BE16-8F9A30185008' -- Financial
            then preselected_products.product_id
        else null
        end as preselected_mealbox_product_id
        , case 
            when products.product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
            then products.product_name
            when products.product_type_id = '288ED8CA-B437-4A6D-BE16-8F9A30185008' -- Financial
            then preselected_products.product_name
        else null
        end as preselected_mealbox_product_name
    from products
    left join mealbox_product_mapping
        on products.product_variation_id = mealbox_product_mapping.product_variation_id
        and products.company_id = mealbox_product_mapping.company_id
    left join products as preselected_products
        on mealbox_product_mapping.preselected_mealbox_product_id = preselected_products.product_id
        and products.company_id = preselected_products.company_id
        and products.meals = preselected_products.meals
        and products.portions = preselected_products.portions

)

, add_unknown_row (

    select 
        md5(
            concat(
                add_preselected_products.product_variation_id,
                add_preselected_products.company_id)
            )
        as pk_dim_products
        , *
    from add_preselected_products

    union all

    select 
        '0'
        , '0'
        , '0'
        , '0'
        , 0
        , '0'
        , '0'
        , "Not relevant"
        , "Not relevant"
        , "Not relevant"
        , "Not relevant"
        , "Not relevant"
        , "0"
        , null
        , null
        , "0"
        , "Not relevant"
)

select * from add_unknown_row