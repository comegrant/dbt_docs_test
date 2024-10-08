with

preference_list as (

    select * from {{ref('int_billing_agreement_preferences_unioned')}}

)

, dim_billing_agreements as (

    select * from {{ref('dim_billing_agreements')}}

)

, dim_preferences as (

    select * from {{ref('dim_preferences')}}

)

, preferences_exploded as (

    select 
    billing_agreement_preferences_updated_id
    , billing_agreement_id
    , company_id
    , preference_id
    from preference_list
    lateral view explode(preference_id_list) as preference_id
    where preference_id is not null


)

, joined as (

    select
    distinct
    md5(concat(
        cast(dim_billing_agreements.pk_dim_billing_agreements as string)
        , cast(preferences_exploded.preference_id as string)
        , cast(preferences_exploded.company_id as string)
        )
    ) as pk_bridge_billing_agreements_preferences
    , dim_billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
    , dim_preferences.pk_dim_preferences as fk_dim_preferences
    , preferences_exploded.billing_agreement_preferences_updated_id
    , preferences_exploded.preference_id
    , preferences_exploded.company_id
    from
    preferences_exploded
    left join dim_billing_agreements
    on preferences_exploded.billing_agreement_preferences_updated_id = dim_billing_agreements.billing_agreement_preferences_updated_id
    left join dim_preferences
    on dim_preferences.preference_id = preferences_exploded.preference_id
    and dim_preferences.company_id = preferences_exploded.company_id
    where dim_billing_agreements.pk_dim_billing_agreements is not null
    and dim_preferences.pk_dim_preferences is not null

)

select * from joined