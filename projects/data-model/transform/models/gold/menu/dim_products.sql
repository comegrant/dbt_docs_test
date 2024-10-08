with 

product_concepts as (

    select * from {{ ref('product_layer__product_concepts') }}

),

product_types_concepts as (

    select * from {{ ref('product_layer__product_types_concepts') }}

),


product_types as (

    select * from {{ ref('product_layer__product_types') }}

),

products as (

    select * from {{ ref('product_layer__products') }}

),

product_statuses as (

    select * from {{ ref('product_layer__product_statuses') }}

),


product_variations as (

    select * from {{ ref('product_layer__product_variations') }}

),

product_variations_companies as (

    select * from {{ ref('product_layer__product_variations_companies') }}

),

meals_and_portions_default as (

    select * from {{ ref('int_product_variations_meal_and_portion_default_values_pivoted') }}

),

meals_and_portions as (

    select * from {{ ref('int_product_variations_meal_and_portion_values_pivoted') }}

),

mealbox_product_mapping as (

    select * from {{ ref('int_product_variations_financial_mealbox_product_mapping') }}

),

product_tables_joined as (

    select
        md5(
            concat(
                product_variations_companies.product_variation_id,
                product_variations_companies.company_id)
            )
        as pk_dim_products

        , product_concepts.product_concept_id
        , product_types.product_type_id
        , products.product_id
        , product_statuses.product_status_id
        , product_variations_companies.product_variation_id
        , product_variations_companies.company_id
        , product_concepts.product_concept_name
        , product_types.product_type_name
        , products.product_name
        , product_statuses.product_status_name
        , product_variations_companies.product_variation_name
        , product_variations.sku
        , coalesce(meals_and_portions.number_of_meals, meals_and_portions_default.number_of_meals) as number_of_meals
        , coalesce(meals_and_portions.number_of_portions, meals_and_portions_default.number_of_portions) as number_of_portions
    from product_variations_companies
    left join product_variations
    on product_variations_companies.product_variation_id = product_variations.product_variation_id
    left join products
    on product_variations.product_id = products.product_id
    left join product_statuses
    on products.product_status_id = product_statuses.product_status_id
    left join product_types
    on products.product_type_id = product_types.product_type_id
    left join product_types_concepts
    on product_types.product_type_id = product_types_concepts.product_type_id
    left join product_concepts
    on product_types_concepts.product_concept_id = product_concepts.product_concept_id
    left join meals_and_portions
    on product_variations_companies.product_variation_id = meals_and_portions.product_variation_id
    and product_variations_companies.company_id = meals_and_portions.company_id
    left join meals_and_portions_default
    on product_types.product_type_id = meals_and_portions_default.product_type_id
)

, product_tables_add_preselected_variation (
    select 
        product_tables_joined.*
        , mealbox_product_mapping.preselected_mealbox_product_id as preselected_mealbox_product_id
        , preselected_variations.product_variation_id as preselected_mealbox_product_variation_id
        , preselected_variations.product_name as preselected_mealbox_product_name
        , preselected_variations.product_variation_name as preselected_mealbox_product_variation_name
    from product_tables_joined
    left join mealbox_product_mapping
        on product_tables_joined.product_variation_id = mealbox_product_mapping.product_variation_id
        and product_tables_joined.company_id = mealbox_product_mapping.company_id
    left join product_tables_joined as preselected_variations
        on mealbox_product_mapping.preselected_mealbox_product_id = preselected_variations.product_id
        and product_tables_joined.company_id = preselected_variations.company_id
        and product_tables_joined.number_of_meals = preselected_variations.number_of_meals
        and product_tables_joined.number_of_portions = preselected_variations.number_of_portions

)

, add_unknown_row (

    select 
        * 
    from product_tables_add_preselected_variation

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