with

source as (

    select * from {{ source('partnership', 'partnership__billing_agreement_partnership') }}

)

, renamed as (

    select

        {# ids #}
        id as billing_agreement_partnership_id
        , company_partnership_id

        {# ints #}
        , agreement_id as billing_agreement_id

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
