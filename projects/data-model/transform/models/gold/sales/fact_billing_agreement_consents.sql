with

consents as (

    select * from {{ ref('cms__billing_agreement_consents') }}

)

, billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, add_fks as (
    select
        md5(
            concat(
                consents.billing_agreement_consent_id
                , consents.valid_from
            )
        )                                                           as pk_fact_billing_agreement_consents
        , consents.is_accepted_consent
        , consents.valid_from
        , consents.billing_agreement_id
        , consents.company_id
        , consents.consent_id
        , billing_agreements.pk_dim_billing_agreements              as fk_dim_billing_agreements
        , cast(date_format(consents.valid_from, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(consents.company_id)                                  as fk_dim_companies
        , md5(consents.consent_id)                                  as fk_dim_consent_types
    from consents
    left join billing_agreements
        on
            consents.billing_agreement_id = billing_agreements.billing_agreement_id
            and consents.valid_from >= billing_agreements.valid_from
            and consents.valid_from < billing_agreements.valid_to
)

select * from add_fks
