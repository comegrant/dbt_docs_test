with dim_preference_combinations as (
    select
        pk_dim_preference_combinations
        , concept_name_combinations                   as concept_combinations
        , taste_name_combinations_including_allergens as taste_preference_combinations
        , allergen_preference_id_list
    from {{ ref('dim_preference_combinations') }}
)

, dim_billing_agreements as (
    select
        pk_dim_billing_agreements
        , company_id
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

, dim_companies as (
    select
        pk_dim_companies
        , company_id
        , language_id
    from {{ ref('dim_companies') }}
)

, final as (
    select
        dim_billing_agreements.*
        , concept_combinations
        , taste_preference_combinations
        , allergen_preference_id_list
    from dim_billing_agreements
    left join
        dim_preference_combinations
        on
            dim_billing_agreements.preference_combination_id
            = dim_preference_combinations.pk_dim_preference_combinations
    left join dim_companies
        on dim_billing_agreements.company_id = dim_companies.pk_dim_companies
    left join latest_consent
        on dim_billing_agreements.billing_agreement_id = latest_consent.billing_agreement_id
    where
        billing_agreement_status_name = 'Active'
        and concept_combinations != 'No concept preferences'
        and latest_consent.is_accepted_consent = true
)

select * from final
