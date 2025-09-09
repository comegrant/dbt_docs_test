with finished_orders as (
    select
        pk_fact_orders
        , company_id
        , language_id
        , billing_agreement_id
        , menu_year
        , menu_week
        , menu_year * 100 + menu_week as menu_yyyyww
        , is_dish
        , is_added_dish
        , is_removed_dish
        , product_variation_id
        , recipe_id
        , fk_dim_products
        , fk_dim_companies
        , fk_dim_recipes
    from {{ ref('fact_orders') }}
    where order_status_id = '4508130E-6BA1-4C14-94A4-A56B074BB135' -- Finished
    and (menu_year * 100 + menu_week) >= 202101
)

, dim_billing_agreements as (
    select
        pk_dim_billing_agreements
        , billing_agreement_id
        , billing_agreement_status_name
        , preference_combination_id
    from {{ ref('dim_billing_agreements') }}
    where is_current = true
)

, fact_billing_agreement_consents as (
    select
        billing_agreement_id
        , is_accepted_consent
        , fk_dim_consent_types
        , valid_from
    from {{ ref('fact_billing_agreement_consents') }}
)

, consent_types as (
    select
        pk_dim_consent_types
        , consent_category_id
    from {{ ref('dim_consent_types') }}
)

, data_processing_consents as (
    select
        fact_billing_agreement_consents.*
    from fact_billing_agreement_consents
    left join consent_types
        on fact_billing_agreement_consents.fk_dim_consent_types = consent_types.pk_dim_consent_types
    where consent_types.consent_category_id = '3495C28B-703C-44AA-B6E0-E01D46684261' -- data processing
)

, consents_with_row_number as (
    select
        *,
        row_number() over(partition by billing_agreement_id order by valid_from desc) as consent_row_number
    from data_processing_consents
)

, latest_consent as (
    select
        *
    from consents_with_row_number
    where consent_row_number = 1
)

, dim_preference_combinations as (
    select
        pk_dim_preference_combinations
        , concept_name_combinations                   as concept_combinations
        , taste_name_combinations_including_allergens as taste_preference_combinations
        , allergen_preference_id_list
    from {{ ref('dim_preference_combinations') }}
)

, dim_recipes as (
    select
        pk_dim_recipes
        , main_recipe_id
        , recipe_id
        , recipe_name
    from
        {{ ref('dim_recipes') }}
)

, dim_products as (
    select
        pk_dim_products
        , product_type_id
        , product_type_name
        , product_variation_name
    from {{ ref('dim_products') }}
)

, final as (
    select
        finished_orders.*
        , main_recipe_id
        , recipe_name
        , product_type_id
        , product_type_name
        , product_variation_name
        , concept_combinations
        , taste_preference_combinations
    from finished_orders
    left join dim_recipes
        on finished_orders.fk_dim_recipes = dim_recipes.pk_dim_recipes
    left join dim_products
        on finished_orders.fk_dim_products = dim_products.pk_dim_products
    left join dim_billing_agreements
        on finished_orders.billing_agreement_id = dim_billing_agreements.billing_agreement_id
    left join dim_preference_combinations
        on
            dim_billing_agreements.preference_combination_id
            = dim_preference_combinations.pk_dim_preference_combinations
    left join latest_consent
        on dim_billing_agreements.billing_agreement_id = latest_consent.billing_agreement_id
    where
        is_dish = true
        and ((is_removed_dish = 0) or (is_removed_dish is null))
        and main_recipe_id is not null
        and billing_agreement_status_name != 'Deleted'
        and concept_combinations != 'No concept preferences'
        and main_recipe_id != 0
        and latest_consent.is_accepted_consent = true
)

select * from final
