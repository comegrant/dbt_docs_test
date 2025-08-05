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
    where
        billing_agreement_status_name = 'Active'
        and concept_combinations != 'No concept preferences'

)

select * from final
