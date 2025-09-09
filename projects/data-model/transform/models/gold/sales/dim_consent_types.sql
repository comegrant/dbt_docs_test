with

consents as (

    select * from {{ ref('cms__consents') }}

)

, consent_categories as (

    select * from {{ ref('cms__consent_categories') }}

)

, consent_tables_joined as (

    select
        md5(consents.consent_id) as pk_dim_consent_types
        , consent_categories.consent_category_name
        , consents.consent_name
        , consents.consent_id
        , consents.consent_category_id
        , consents.company_id
    from consents
    left join consent_categories
        on consents.consent_category_id = consent_categories.consent_category_id

)

select * from consent_tables_joined