
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
        , coalesce(meals_and_portions.meals, meals_and_portions_default.meals) as meals
        , coalesce(meals_and_portions.portions, meals_and_portions_default.portions) as portions
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

select * from product_tables_joined