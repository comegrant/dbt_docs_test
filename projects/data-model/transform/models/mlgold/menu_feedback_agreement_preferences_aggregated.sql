with

agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}

)

, preference_combinations as (

    select * from {{ ref('dim_preference_combinations') }}

)

, billing_agreement_preferences as (

    select
        agreements.company_id
        , agreements.billing_agreement_id
        , agreements.billing_agreement_status_name
        , preference_combinations.taste_name_combinations_including_allergens
        , preference_combinations.taste_preferences_including_allergens_id_list
    from agreements
    left join preferences
        on
            agreements.billing_agreement_preferences_updated_id
            = preferences.billing_agreement_preferences_updated_id
    left join preference_combinations
        on
            preferences.preference_combination_id
            = preference_combinations.pk_dim_preference_combinations
    where
        agreements.is_current = true
        and agreements.billing_agreement_status_name = 'Active'
        and preference_combinations.taste_preferences_including_allergens_id_list is not null
)

, agreement_preferences_aggregated as (

    select
        company_id
        , md5(concat_ws(
            '-'
            , company_id
            , taste_name_combinations_including_allergens
        ))                                                   as negative_taste_preference_combo_id
        , lower(taste_name_combinations_including_allergens) as negative_taste_preferences
        , taste_preferences_including_allergens_id_list      as negative_taste_preferences_ids
        , count(distinct billing_agreement_id)               as number_of_users
    from billing_agreement_preferences
    group by 1, 2, 3, 4
)

select * from agreement_preferences_aggregated
