with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, financial_mealbox_product_mapping as (

    select * from {{ ref('int_product_variations_financial_mealbox_product_mapping') }}

)

, financial_product_deviations as (

    select
        deviations.billing_agreement_id
        , deviations.billing_agreement_basket_id
        , deviations.company_id
        , deviations.product_variation_id
        , products.product_id
        , products.meals
        , products.portions
        , case 
            -- Use updated at timestamp when Tech overwrites already existing deviation products using script (e.g., in migration processes)
            when deviations.deviation_product_updated_by like '%Tech%' then deviations.deviation_product_updated_at
            else deviations.deviation_created_at 
        end as valid_from
        ,deviations.billing_agreement_basket_deviation_origin_id
        , 'financial' as basket_source
    from deviations
    -- only include deviations with financial products
    inner join products
        on
            deviations.product_variation_id = products.product_variation_id
            and deviations.company_id = products.company_id
            and products.product_type_id = '{{ var("financial_product_type_id") }}'


)

-- find the mealbox product variation that corresponds to the financial product variation
, financial_product_deviations_mealbox_mapping as (

    select
        financial_product_deviations.billing_agreement_id
        , financial_product_deviations.billing_agreement_basket_id
        , financial_product_deviations.company_id
        , financial_product_deviations.product_variation_id as financial_product_variation_id
        , financial_mealbox_product_mapping.preselected_mealbox_product_id as product_id
        , financial_product_deviations.meals
        , financial_product_deviations.portions
        , products.product_variation_id
        , financial_product_deviations.valid_from
        , financial_product_deviations.billing_agreement_basket_deviation_origin_id
    from financial_product_deviations
    left join financial_mealbox_product_mapping
        on financial_product_deviations.product_variation_id = financial_mealbox_product_mapping.product_variation_id
        and financial_product_deviations.company_id = financial_mealbox_product_mapping.company_id
    left join products
        on financial_mealbox_product_mapping.preselected_mealbox_product_id = products.product_id
        and financial_product_deviations.company_id = products.company_id
        and financial_product_deviations.meals = products.meals
        and financial_product_deviations.portions = products.portions
    where products.product_variation_id is not null

)

select * from financial_product_deviations_mealbox_mapping
