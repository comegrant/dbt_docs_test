with

source as (

    select * from {{ ref('scd_cms__billing_agreement_consents') }}

)

, renamed as (

    select

    {# ids #}
        billing_agreement_consent_id
        , agreement_id                                       as billing_agreement_id
        , consent_id
        , company_id

        {# booleans #}
        , is_accepted as is_accepted_consent

        {# scd #}
        , dbt_valid_from as valid_from
        , {{ get_scd_valid_to('dbt_valid_to') }} as valid_to

        {# system #}
        , created_by                                         as source_created_by
        , updated_at                                         as source_updated_at
        , updated_by                                         as source_updated_by
        , convert_timezone('Europe/Oslo', 'UTC', created_at) as source_created_at

    from source

)

select * from renamed