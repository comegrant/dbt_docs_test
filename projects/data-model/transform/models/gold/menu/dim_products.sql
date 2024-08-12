with 

product_concepts as (

    select * from {{ ref('sil_product_layer__product_concepts') }}

),

product_types_concepts as (

    select * from {{ ref('sil_product_layer__product_types_concepts') }}

),


product_types as (

    select * from {{ ref('sil_product_layer__product_types') }}

),

products as (

    select * from {{ ref('sil_product_layer__products') }}

),

product_status as (

    select * from {{ ref('sil_product_layer__product_status') }}

),


product_variations as (

    select * from {{ ref('sil_product_layer__product_variations') }}

),

product_variations_companies as (

    select * from {{ ref('sil_product_layer__product_variations_companies') }}

),

attribute_templates as (

    select * from {{ ref('int_attribute_templates_pivoted') }}

),

attribute_values as (

    select * from {{ ref('int_attribute_values_pivoted') }}

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
        , product_status.product_status_id
        , product_variations_companies.product_variation_id
        , product_variations_companies.company_id
        , product_concepts.product_concept_name
        , product_types.product_type_name
        , products.product_name
        , product_status.product_status_name
        , product_variations_companies.product_variation_name
        , product_variations.sku
        , coalesce(attribute_values.number_of_meals, attribute_templates.number_of_meals) as number_of_meals
        , coalesce(attribute_values.number_of_portions, attribute_templates.number_of_portions) as number_of_portions
        , attribute_values.preselected_mealbox_product_id
    from product_variations_companies
    left join product_variations
    on product_variations_companies.product_variation_id = product_variations.product_variation_id
    left join products
    on product_variations.product_id = products.product_id
    left join product_status
    on products.product_status_id = product_status.product_status_id
    left join product_types
    on products.product_type_id = product_types.product_type_id
    left join product_types_concepts
    on product_types.product_type_id = product_types_concepts.product_type_id
    left join product_concepts
    on product_types_concepts.product_concept_id = product_concepts.product_concept_id
    left join attribute_values
    on product_variations_companies.product_variation_id = attribute_values.product_variation_id
    and product_variations_companies.company_id = attribute_values.company_id
    left join attribute_templates
    on product_types.product_type_id = attribute_templates.product_type_id
)

select * from product_tables_joined