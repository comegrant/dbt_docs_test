with 

source as (

    select * from {{ ref('cms__loyalty_points') }}

)

, add_pk as (

    select
        md5(loyalty_points_id) as pk_fact_loyalty_points
        , source.*
    from source

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, billing_agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}
  
)

, add_fks as (

    select
        add_pk.pk_fact_loyalty_points
        , add_pk.loyalty_points_id
        , add_pk.loyalty_points_parent_id
        , add_pk.billing_agreement_id
        , add_pk.loyalty_event_id
        , add_pk.loyalty_order_id
        , add_pk.transaction_reason
        , add_pk.transaction_points
        , add_pk.remaining_points
        , add_pk.billing_agreement_order_id
        , agreements.company_id
        , add_pk.source_created_at
        , add_pk.points_expiration_date
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
        , md5(add_pk.loyalty_event_id) as fk_dim_loyalty_events
        , cast(date_format(add_pk.source_created_at, 'yyyyMMdd') as int) as fk_dim_dates_transaction
        , cast(date_format(add_pk.points_expiration_date, 'yyyyMMdd') as int) as fk_dim_dates_points_expiration
        , md5(agreements.company_id) as fk_dim_companies
    from add_pk
    left join agreements 
        on add_pk.billing_agreement_id = agreements.billing_agreement_id
        and add_pk.source_created_at >= agreements.valid_from
        and add_pk.source_created_at < agreements.valid_to
    left join billing_agreement_preferences
        on agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id

)

select * from add_fks
