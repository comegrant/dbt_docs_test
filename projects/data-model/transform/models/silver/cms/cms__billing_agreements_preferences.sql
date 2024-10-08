with 

source as (

    select * from {{ ref('scd_cms__billing_agreement_preferences') }}

)

, renamed as (

    select

        
        {# ids #}
        id as billing_agreements_preferences_id
        , agreement_id as billing_agreement_id
        , upper(preference_id) as preference_id

        {# scd #}
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
