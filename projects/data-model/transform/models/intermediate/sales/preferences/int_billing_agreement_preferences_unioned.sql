with

registered as (
    
    select * from {{ref('int_billing_agreement_preferences_at_signup')}}
    
)

, postgres as (

    select * from {{ref('int_billing_agreement_preferences_postgres')}}

)

, current as (

    select * from {{ref('int_billing_agreement_preferences_complemented_with_subscribed_mealbox')}}

)

, postgres_min_valid_from as (
    select 
    billing_agreement_id
    , min(valid_from) as min_valid_from
    from postgres
    group by 1
)

, current_min_valid_from as (
    select 
    billing_agreement_id
    , min(valid_from) as min_valid_from
    from current
    group by 1
)

, registered_before_postgres_and_current as (
    select
    registered.*
    from registered
    left join postgres_min_valid_from
    on postgres_min_valid_from.billing_agreement_id = registered.billing_agreement_id
    left join current_min_valid_from
    on current_min_valid_from.billing_agreement_id = registered.billing_agreement_id
    where registered.valid_from < postgres_min_valid_from.min_valid_from and registered.valid_from < current_min_valid_from.min_valid_from
)

, postgres_before_current as (
    select
    postgres.*
    from postgres
    left join current_min_valid_from
    on current_min_valid_from.billing_agreement_id = postgres.billing_agreement_id
    where postgres.valid_from < current_min_valid_from.min_valid_from
)

, unioned as (
    select * from registered_before_postgres_and_current
    union all
    select * from postgres_before_current
    union all
    select * from current
)

, updated_valid_to as (

    select 
    md5(concat(
        cast(billing_agreement_id as string)
        , cast(valid_from as string)
    )) as billing_agreement_preferences_updated_id
    , billing_agreement_id
    , company_id
    , array_sort(filter(preference_id_list, x -> x is not null)) as preference_id_list
    , valid_from
    , {{ get_scd_valid_to('valid_from', 'billing_agreement_id') }} as valid_to
    , source
    from unioned
)

, preference_combination_id_added (
    select
        billing_agreement_preferences_updated_id
        , billing_agreement_id
        , company_id
        , md5(array_join(preference_id_list, ',')) as preference_combination_id
        , preference_id_list
        , valid_from
        , valid_to
        , source
    from updated_valid_to
)

select * from preference_combination_id_added