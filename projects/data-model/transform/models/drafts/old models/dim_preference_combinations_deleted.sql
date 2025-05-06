with

bridge as (

    select * from {{ref('bridge_billing_agreements_preferences')}}

)

, dim_preferences as (

    select * from {{ref('dim_preferences')}}

)

, bridge_and_dimension_joined as (

    select

    bridge.fk_dim_billing_agreements
    , bridge.billing_agreement_preferences_updated_id
    , dim_preferences.company_id
    , dim_preferences.preference_id
    , dim_preferences.preference_name
    , dim_preferences.preference_type_id

    from bridge
    left join dim_preferences
        on bridge.fk_dim_preferences = dim_preferences.pk_dim_preferences

)

, preference_lists_per_type as (

    select
    fk_dim_billing_agreements
    , company_id
    , preference_type_id
    , array_sort(collect_list(preference_name)) as preference_name_list_per_type
    , array_sort(collect_list(preference_id)) as preference_id_list_per_type

    from bridge_and_dimension_joined
    group by 1,2,3

)

, preference_lists_all_types as (

    select
    fk_dim_billing_agreements
    , company_id
    , array_sort(collect_list(preference_name)) as preference_name_list_all_types
    , array_sort(collect_list(preference_id)) as preference_id_list_all_types

    from bridge_and_dimension_joined
    group by 1,2

)

, preference_lists_all_types_and_per_type as (

    select
    preference_lists_all_types.fk_dim_billing_agreements
    , preference_lists_all_types.company_id
    , preference_lists_all_types.preference_name_list_all_types
    , preference_lists_all_types.preference_id_list_all_types

    , preference_lists_concept.preference_name_list_per_type as preference_name_list_concept_type
    , preference_lists_concept.preference_id_list_per_type as preference_id_list_concept_type

    , preference_lists_taste.preference_name_list_per_type as preference_name_list_taste_type
    , preference_lists_taste.preference_id_list_per_type as preference_id_list_taste_type

    from preference_lists_all_types

    left join preference_lists_per_type as preference_lists_concept
        on preference_lists_all_types.fk_dim_billing_agreements = preference_lists_concept.fk_dim_billing_agreements
        and preference_lists_concept.preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' --concept

    left join preference_lists_per_type as preference_lists_taste
        on preference_lists_all_types.fk_dim_billing_agreements = preference_lists_taste.fk_dim_billing_agreements
        and preference_lists_taste.preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB' --taste

)

, convert_arrays_to_strings as (

    select

    fk_dim_billing_agreements as pk_dim_preference_combinations
    , fk_dim_billing_agreements
    , coalesce(
        array_join(preference_name_list_all_types,', ')
        , 'No preferences'
     ) as preference_combinations
    , array_join(preference_id_list_all_types,', ') as preference_id_combinations
    , coalesce(
        array_join(preference_name_list_concept_type,', ')
        , 'No concept preferences'
     ) as concept_combinations
    , array_join(preference_id_list_concept_type,', ') as preference_id_combinations_concept_type
    , coalesce(
        array_join(preference_name_list_taste_type,', ')
        , 'No taste preferences'
     ) as taste_preference_combinations
    , array_join(preference_id_list_taste_type,', ') as preference_id_combinations_taste_type
    , case
        when preference_name_list_all_types is null then 0
        else size(preference_name_list_all_types)
    end as number_of_preferences
    , case
        when preference_name_list_concept_type is null then 0
        else size(preference_name_list_concept_type)
    end as number_of_concept_preferences
    , case
        when preference_name_list_taste_type is null then 0
        else size(preference_name_list_taste_type)
    end as number_of_taste_preferences

    from preference_lists_all_types_and_per_type
)

select * from convert_arrays_to_strings
