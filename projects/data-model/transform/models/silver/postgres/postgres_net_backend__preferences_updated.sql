with 

preferences_godtlevert as (

    select * from {{ ref('base_postgres_net_backend_godtlevert__ce_preferences_updated') }}

)

, preferences_adams as (

    select * from {{ ref('base_postgres_net_backend_adams__ce_preferences_updated') }}

)

, preferences_linas as (

    select * from {{ ref('base_postgres_net_backend_linas__ce_preferences_updated') }}

)

, preferences_retnemt as (

    select * from {{ ref('base_postgres_net_backend_retnemt__ce_preferences_updated') }}

)

, preferences_unioned as (

    select 
    *
    , '09ECD4F0-AE58-4539-8E8F-9275B1859A19' as company_id
    from preferences_godtlevert
    
    union all
    
    select 
    * 
    , '8A613C15-35E4-471F-91CC-972F933331D7' as company_id
    from preferences_adams
    
    union all
    
    select 
    *
    , '6A2D0B60-84D6-4830-9945-58D518D27AC2' as company_id
    from preferences_linas

    union all
    
    select 
    *
    , '5E65A955-7B1A-446C-B24F-CFE576BF52D7' as company_id
    from preferences_retnemt

)

, renamed as (

    select

        
        {# ids #}
        md5(concat(
              cast(id as string)
            , cast(company_id as string)
            )
        ) AS preference_updated_id
        , id as ce_preference_updated_id
        , cast(agreement_id as int) as billing_agreement_id
        , company_id
        , upper(concept_preference_id) as concept_preference_id
        , concept_preferences as concept_preference_list
        , taste_preferences as taste_preference_list
        
        {# system #}
        , updated_at as source_updated_at

    from preferences_unioned

)


select * from renamed
