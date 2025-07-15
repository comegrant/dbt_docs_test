with

agreements as (

    select * from {{ ref('dim_billing_agreements') }}
    where
        is_current = true
        and billing_agreement_status_name = 'Active'

)

, preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}

)

, preference_combinations as (

    select * from {{ ref('dim_preference_combinations') }}

)

, products as (

    select * from {{ ref('dim_products') }}
    where product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox

)

, bridge_agreement_products as (

    select * from {{ ref('bridge_billing_agreements_basket_products') }}
)

, billing_agreement_portions as (

    select
        agreements.billing_agreement_id
        , products.portions
    from agreements
    left join bridge_agreement_products
        on agreements.pk_dim_billing_agreements = bridge_agreement_products.fk_dim_billing_agreements
    left join products
        on bridge_agreement_products.fk_dim_products = products.pk_dim_products

)

, billing_agreement_preferences as (

    select
        agreements.company_id
        , agreements.billing_agreement_id
        , agreements.billing_agreement_status_name
        , billing_agreement_portions.portions
        , preference_combinations.taste_name_combinations_including_allergens
        , preference_combinations.taste_preferences_including_allergens_id_list
    from agreements
    left join billing_agreement_portions
        on agreements.billing_agreement_id = billing_agreement_portions.billing_agreement_id
    left join preferences
        on
            agreements.billing_agreement_preferences_updated_id
            = preferences.billing_agreement_preferences_updated_id
    left join preference_combinations
        on
            preferences.preference_combination_id
            = preference_combinations.pk_dim_preference_combinations
    where preference_combinations.taste_preferences_including_allergens_id_list is not null
)

, agreement_preferences_aggregated as (

    select
        company_id
        , md5(
            concat_ws(
                '-'
                , company_id
                , taste_name_combinations_including_allergens
            )
        )                                                    as negative_taste_preference_combo_id
        , lower(taste_name_combinations_including_allergens) as negative_taste_preferences
        , taste_preferences_including_allergens_id_list      as negative_taste_preferences_ids
        , count(distinct billing_agreement_id)               as number_of_users
        , count(
            distinct case when portions = 1 then billing_agreement_id end
        )                                                    as users_with_1_portions
        , count(
            distinct case when portions = 2 then billing_agreement_id end
        )                                                    as users_with_2_portions
        , count(
            distinct case when portions = 3 then billing_agreement_id end
        )                                                    as users_with_3_portions
        , count(
            distinct case when portions = 4 then billing_agreement_id end
        )                                                    as users_with_4_portions
        , count(
            distinct case when portions = 5 then billing_agreement_id end
        )                                                    as users_with_5_portions
        , count(
            distinct case when portions = 6 then billing_agreement_id end
        )                                                    as users_with_6_portions
    from billing_agreement_preferences
    group by 1, 2, 3, 4
)

select * from agreement_preferences_aggregated
