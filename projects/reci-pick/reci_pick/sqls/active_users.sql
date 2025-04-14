with dim_preference_combinations as (
    select
        pk_dim_preference_combinations,
        concept_combinations,
        preference_id_combinations_concept_type,
        taste_preference_combinations,
        preference_id_combinations_taste_type,
        fk_dim_billing_agreements
    from {env}.gold.dim_preference_combinations
),

dim_billing_agreements as (
    select
        pk_dim_billing_agreements,
        company_id,
        billing_agreement_id,
        billing_agreement_status_name
    from {env}.gold.dim_billing_agreements
    where is_current = true
),

dim_companies as (
    select
        pk_dim_companies,
        company_id,
        language_id
    from {env}.gold.dim_companies
)

select
    dim_billing_agreements.*,
    concept_combinations,
    taste_preference_combinations
from dim_billing_agreements
left join
    dim_preference_combinations
on dim_billing_agreements.pk_dim_billing_agreements = dim_preference_combinations.fk_dim_billing_agreements
    left join dim_companies
on dim_companies.pk_dim_companies = dim_billing_agreements.company_id
where billing_agreement_status_name = 'Active'
    and dim_billing_agreements.company_id = '{company_id}'
    and concept_combinations != "No concept preferences"
