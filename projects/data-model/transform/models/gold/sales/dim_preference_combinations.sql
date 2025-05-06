with

recipe_preference as (

    select * from {{ ref('int_recipe_preferences_unioned') }}

)

, billing_agreement_preference as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}

)

, preferences as (

    select * from {{ ref('cms__preferences') }}

)

, all_preference_combinations as (
    select
        preference_combination_id
        , preference_id_list
    from recipe_preference

    union all

    select
        preference_combination_id
        , preference_id_list
    from billing_agreement_preference
)

, unique_combinations as (
    select
        preference_combination_id
        , preference_id_list --all combinations
    from all_preference_combinations
    group by 1, 2
)

, preferences_exploded as (
    select
        preference_combination_id
        , preference_id
    from unique_combinations
        lateral view explode(preference_id_list) as preference_id
    where preference_id is not null
)

, preferences_info as (
    select
        preferences_exploded.preference_combination_id
        , preferences_exploded.preference_id
        , preferences.preference_name
        , preferences.preference_type_id
        , preferences.is_allergen
    from preferences_exploded
    left join preferences
        on preferences_exploded.preference_id = preferences.preference_id
)

, map_preference_type as (
    select
        preference_combination_id
        , preference_id
        , preference_name
        , case
            when is_allergen = true then 'allergen_preference'
            when
                is_allergen = false and preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB'
                then 'taste_preference_excluding_allergens'
            when preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' then 'concept_preference'
        end as preference_type
    from preferences_info
)

, preference_type_lists as (
    select
        preference_combination_id
        , collect_list(
            case when preference_type = 'allergen_preference' then preference_id end
        )                                       as allergen_preference_id_list
        , collect_list(
            case when preference_type = 'concept_preference' then preference_id end
        )                                       as concept_preference_id_list
        , collect_list(
            case when preference_type = 'taste_preference_excluding_allergens' then preference_id end
        )                                       as taste_preferences_excluding_allergens_id_list
        , collect_list(
            case
                when preference_type = 'allergen_preference'
                    or preference_type = 'taste_preference_excluding_allergens'
                then preference_id
            end
        )  as taste_preferences_including_allergens_id_list
        , collect_list(preference_name) as preferences_name_list
        , collect_list(
            case when preference_type = 'allergen_preference' then preference_name end
        )                                       as allergen_preferences_name_list
        , collect_list(
            case when preference_type = 'concept_preference' then preference_name end
        )                                       as concept_preferences_name_list
        , collect_list(
            case when preference_type = 'taste_preference_excluding_allergens' then preference_name end
        )                                       as taste_preferences_excluding_allergens_name_list
        , collect_list(
            case
                when preference_type = 'allergen_preference'
                    or preference_type = 'taste_preference_excluding_allergens'
                then preference_name
            end
        ) as taste_preferences_including_allergens_name_list
    from map_preference_type
    group by 1
)

, add_complete_preference_list as (
    select
        unique_combinations.preference_combination_id
        , unique_combinations.preference_id_list
        , preference_type_lists.allergen_preference_id_list
        , preference_type_lists.concept_preference_id_list
        , preference_type_lists.taste_preferences_excluding_allergens_id_list
        , preference_type_lists.taste_preferences_including_allergens_id_list
        , preference_type_lists.preferences_name_list
        , preference_type_lists.allergen_preferences_name_list
        , preference_type_lists.concept_preferences_name_list
        , preference_type_lists.taste_preferences_excluding_allergens_name_list
        , preference_type_lists.taste_preferences_including_allergens_name_list
    from unique_combinations
    left join preference_type_lists
        on unique_combinations.preference_combination_id = preference_type_lists.preference_combination_id
)

, convert_arrays_to_strings as (
    select
        preference_combination_id as pk_dim_preference_combinations
        , preference_combination_id
        , preference_id_list as all_preference_id_list
        , allergen_preference_id_list
        , concept_preference_id_list
        , taste_preferences_excluding_allergens_id_list
        , taste_preferences_including_allergens_id_list
        , coalesce(
            nullif(array_join(preferences_name_list, ', '), '')
            , 'No preferences'
        )   as preference_name_combinations
        , coalesce(
            nullif(array_join(allergen_preferences_name_list, ', '), '')
            , 'No allergen preferences'
        )   as allergen_name_combinations
        , coalesce(
            nullif(array_join(concept_preferences_name_list, ', '), '')
            , 'No concept preferences'
        )   as concept_name_combinations
        , coalesce(
            nullif(array_join(taste_preferences_excluding_allergens_name_list, ', '), '')
            , 'No taste preferences excluding allergens'
        )   as taste_name_combinations_excluding_allergens
        , coalesce(
            nullif(array_join(taste_preferences_including_allergens_name_list, ', '), '')
            , 'No taste preferences including allergens'
        )   as taste_name_combinations_including_allergens
        , case
            when preference_id_list is null then 0
            else size(preference_id_list)
        end as number_of_preferences
        , case
            when allergen_preference_id_list is null then 0
            else size(allergen_preference_id_list)
        end as number_of_allergen_preferences
        , case
            when concept_preference_id_list is null then 0
            else size(concept_preference_id_list)
        end as number_of_concept_preferences
        , case
            when taste_preferences_excluding_allergens_id_list is null then 0
            else size(taste_preferences_excluding_allergens_id_list)
        end as number_of_taste_preferences_excluding_allergens
        , case
            when taste_preferences_including_allergens_id_list is null then 0
            else size(taste_preferences_including_allergens_id_list)
        end as number_of_taste_preferences_including_allergens
    from add_complete_preference_list
)

select * from convert_arrays_to_strings
