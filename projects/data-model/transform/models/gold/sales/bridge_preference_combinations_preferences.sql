with

preference_combinations as (

    select * from {{ref('dim_all_preference_combinations')}}

)

, preferences as (

    select * from {{ref('dim_preferences')}}

)

, joined as (

    select
    md5(concat(
        cast(preference_combinations.pk_preference_combination_id as string)
        , cast(preferences.preference_id as string)
        , cast(company_id as string)
        )
    ) as pk_bridge_preference_combinations_preferences
    , preference_combinations.pk_preference_combination_id as fk_dim_preference_combinations
    , preferences.pk_dim_preferences as fk_dim_preferences
    , preference_combinations.all_preference_id_list as preference_id_list
    , preferences.preference_id
    from preference_combinations
    left join preferences
        on array_contains(
            preference_combinations.all_preference_id_list
            , preferences.preference_id
        )
    where size(preference_combinations.all_preference_id_list) > 0

)

select * from joined
