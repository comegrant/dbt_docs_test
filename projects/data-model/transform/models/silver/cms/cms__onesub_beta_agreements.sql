with

source as (
    select * from {{ source('cms', 'cms__onesub_beta_agreements') }}
)

, renamed as (
    select

    {# ids #}
        agreement_id as billing_agreement_id
        , is_internal

    from source
)

select * from renamed
