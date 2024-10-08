with 

source as (

    select * from {{ ref('scd_cms__billing_agreements_preferences_list') }}

)

, renamed as (

    select

        
        {# ids #}
        agreement_id as billing_agreement_id

        {# objects #}
        , preference_id_list
        , billing_agreement_preference_id_list

        {# scd #}
        , dbt_valid_from as valid_from
        , {{ get_scd_valid_to('dbt_valid_to') }} as valid_to
        
        {# system #}
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
