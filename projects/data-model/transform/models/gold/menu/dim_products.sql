with 

products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, mealbox_product_mapping as (

    select * from {{ ref('int_product_variations_financial_mealbox_product_mapping') }}

)

, add_preselected_variation (
    select 
        products.*
        , mealbox_product_mapping.preselected_mealbox_product_id as preselected_mealbox_product_id
        , preselected_variations.product_variation_id as preselected_mealbox_product_variation_id
        , preselected_variations.product_name as preselected_mealbox_product_name
        , preselected_variations.product_variation_name as preselected_mealbox_product_variation_name
    from products
    left join mealbox_product_mapping
        on products.product_variation_id = mealbox_product_mapping.product_variation_id
        and products.company_id = mealbox_product_mapping.company_id
    left join products as preselected_variations
        on mealbox_product_mapping.preselected_mealbox_product_id = preselected_variations.product_id
        and products.company_id = preselected_variations.company_id
        and products.meals = preselected_variations.meals
        and products.portions = preselected_variations.portions

)

, add_unknown_row (

    select 
        md5(
            concat(
                add_preselected_variation.product_variation_id,
                add_preselected_variation.company_id)
            )
        as pk_dim_products
        , *
    from add_preselected_variation

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
        , "0"
        , "Not relevant"
        , "Not relevant"
)

select * from add_unknown_row