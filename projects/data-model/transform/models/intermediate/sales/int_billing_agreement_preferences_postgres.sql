with

preferences_updated as (

    select * from {{ref('postgres_net_backend__preferences_updated')}}
    
)

, preferences_parsed_json as (

    select 
        preference_updated_id
        , ce_preference_updated_id
        , billing_agreement_id
        , company_id
        , concept_preference_id
        , transform(from_json(taste_preference_list, 'array<struct<tastePreferenceId:string>>'), x -> upper(x['tastePreferenceId'])) as taste_preference_array
        , transform(from_json(concept_preference_list, 'array<struct<conceptPreferenceId:string>>'), x -> upper(x['conceptPreferenceId'])) as concept_preference_array
        , source_updated_at as valid_from
        , {{ get_scd_valid_to('source_updated_at', 'billing_agreement_id') }} as valid_to
    from preferences_updated 

)

, taste_and_concept_array_unioned as (
    select 
        billing_agreement_id
        , company_id
        , array_union(
            array_union(coalesce(concept_preference_array,array()), array(upper(concept_preference_id)))
            , coalesce(taste_preference_array,array())
         ) as preference_id_list
        , valid_from
        , valid_to
        ,'postgres_net_backend__preferences_updated' as source
    from preferences_parsed_json
    
)

select * from taste_and_concept_array_unioned