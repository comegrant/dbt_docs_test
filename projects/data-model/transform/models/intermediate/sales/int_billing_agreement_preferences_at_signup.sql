with

basket_registered as (

    select * from {{ref('cms__register_info_baskets')}}

)

, products_registered as (

    select * from {{ref('cms__register_info_products')}}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, billing_agreements as (
    select * from {{ref('cms__billing_agreements')}}
    where valid_to is null
)

, preference_attributes as (

    select * from {{ref('cms__preference_attribute_values')}}

)

, basket_registered_scd as (
    select 
    register_info_basket_id
    , billing_agreement_id
    , start_date as valid_from
    , {{ get_scd_valid_to() }} as valid_to
    from basket_registered
)

, basket_and_products_joined as (
    select 
    billing_agreement_id
    , product_variation_id
    , valid_from
    , valid_to
    from basket_registered_scd
    left join products_registered
    on products_registered.register_info_basket_id = basket_registered_scd.register_info_basket_id

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
    basket_and_products_joined.billing_agreement_id
    , billing_agreements.company_id
    , array(preferences_and_products_mapped.preference_id) as preference_id_list
    , basket_and_products_joined.valid_from
    , basket_and_products_joined.valid_to
    , 'cms__register_info_products' as source
    from basket_and_products_joined
    left join billing_agreements
    on billing_agreements.billing_agreement_id = basket_and_products_joined.billing_agreement_id
    left join products
    on products.product_variation_id = basket_and_products_joined.product_variation_id
    and billing_agreements.company_id = products.company_id
    left join preferences_and_products_mapped
    on preferences_and_products_mapped.product_id = products.product_id
)

, billing_agreement_preferences_filter_on_valid_from as (
    --This is done since there was no events before march 2023 so the preferences might be wrong before this date.
    select * from billing_agreement_preferences_list
    where valid_from > '2023-03-01'
)

select * from billing_agreement_preferences_filter_on_valid_from