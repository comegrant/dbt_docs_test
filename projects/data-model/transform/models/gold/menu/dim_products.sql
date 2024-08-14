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

product_status as (

    select * from {{ ref('product_layer__product_status') }}

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

corresponding_mealboxes as (

    select * from {{ ref('int_product_variations_corresponding_mealbox_ids_pivoted') }}

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
        , coalesce(meals_and_portions.number_of_meals, meals_and_portions_default.number_of_meals) as number_of_meals
        , coalesce(meals_and_portions.number_of_portions, meals_and_portions_default.number_of_portions) as number_of_portions
        , corresponding_mealboxes.preselected_mealbox_product_id
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
    left join meals_and_portions
    on product_variations_companies.product_variation_id = meals_and_portions.product_variation_id
    and product_variations_companies.company_id = meals_and_portions.company_id
    left join meals_and_portions_default
    on product_types.product_type_id = meals_and_portions_default.product_type_id
    left join corresponding_mealboxes
    on product_variations_companies.product_variation_id = corresponding_mealboxes.product_variation_id
    and product_variations_companies.company_id = corresponding_mealboxes.company_id
)

select * from product_tables_joined