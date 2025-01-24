with

source as (

    select * from {{ source('cms', 'cms__consent') }}

)

, renamed as (

    select

    {# ids #}
        id                    as consent_id
        , company_id
        , category_consent_id as consent_category_id

        {# strings #}
        , name                as consent_name

        {# system #}
        , created_at          as source_created_at
        , created_by          as source_created_by
        , updated_at          as source_updated_at
        , updated_by          as source_updated_by


    from source

)

select * from renamed