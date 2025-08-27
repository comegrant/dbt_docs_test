
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

attributes_template as (

    select * from {{ ref('int_product_variation_attribute_templates_pivoted') }}

),

attributes as (

    select * from {{ ref('int_product_variation_attribute_values_pivoted') }}

),

product_tables_joined as (

    select
        product_concepts.product_concept_id
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
        , product_variations_companies.sent_to_frontend
        , coalesce(attributes.meals, attributes_template.meals) as meals
        , coalesce(attributes.portions, attributes_template.portions) as portions
        , coalesce(attributes.portion_name, attributes_template.portion_name) as portion_name
        , coalesce(attributes.is_active_product, attributes_template.is_active_product) as is_active_product
        , coalesce(attributes.vat_rate, attributes_template.vat_rate) as vat_rate
        , coalesce(attributes.picking_line_label, attributes_template.picking_line_label) as picking_line_label
        , coalesce(attributes.maximum_order_quantity, attributes_template.maximum_order_quantity) as maximum_order_quantity
        , case 
            when coalesce(attributes.portion_name, attributes_template.portion_name) like '%+%'
            then true
            else false
        end as has_extra_protein

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
    left join attributes
        on product_variations_companies.product_variation_id = attributes.product_variation_id
        and product_variations_companies.company_id = attributes.company_id
    left join attributes_template
        on product_types.product_type_id = attributes_template.product_type_id
)

select * from product_tables_joined