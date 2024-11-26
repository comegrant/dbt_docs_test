with

register_products as (

    select * from {{ref('int_basket_products_at_signup')}}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, preference_attributes as (

    select * from {{ref('cms__preference_attribute_values')}}

)

, preferences_and_products_mapped as (

    select 
    preference_id
    , attribute_value as product_id
    from preference_attributes
    where attribute_id = '680D1B8A-F967-4713-A270-8E30D683F4B8' --linked_mealbox_id
)

, billing_agreement_preferences_list as (

    select 
    register_products.billing_agreement_id
    , register_products.company_id
    , array(preferences_and_products_mapped.preference_id) as preference_id_list
    , register_products.valid_from
    , register_products.valid_to
    , 'cms__register_info_products' as source
    from register_products
    left join products
    on products.product_variation_id = register_products.product_variation_id
    and register_products.company_id = products.company_id
    left join preferences_and_products_mapped
    on preferences_and_products_mapped.product_id = products.product_id
)

, billing_agreement_preferences_filter_on_valid_from as (
    --This is done since there was no events before march 2023 so the preferences might be wrong before this date.
    select * from billing_agreement_preferences_list
    where valid_from > '2023-03-01'
)

select * from billing_agreement_preferences_filter_on_valid_from