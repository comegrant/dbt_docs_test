with 

loyalty_points as (

    select * from {{ ref('cms__loyalty_points') }}

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, loyalty_level_companies as (

    select * from {{ ref('int_loyalty_level_companies_loyalty_levels_joined') }}

)

, loyalty_seasons as (

    select * from {{ ref('dim_loyalty_seasons') }}

)

, billing_agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}
  
)

-- Calculate points earned from multipliers. Points earned from multipliers are earned only on orders placed at order gen 
, points_from_multipliers as (

    select 
        md5(loyalty_points_id) as pk_fact_loyalty_points
        , loyalty_level_companies.loyalty_level_number
        , loyalty_level_companies.point_multiplier_spendable as level_multiplier
        , cast(ceiling(transaction_points * (1-1/point_multiplier_spendable)) as int) as level_booster_points
    
    from loyalty_points

    left join agreements 
        on loyalty_points.billing_agreement_id = agreements.billing_agreement_id
        and loyalty_points.source_created_at >= agreements.valid_from
        and loyalty_points.source_created_at < agreements.valid_to

    left join loyalty_level_companies
        on agreements.company_id = loyalty_level_companies.company_id
        and loyalty_points.source_created_at >= loyalty_level_companies.valid_from
        and loyalty_points.source_created_at < loyalty_level_companies.valid_to
        and agreements.loyalty_level_number = loyalty_level_companies.loyalty_level_number

    where loyalty_points.loyalty_event_id = 'E4D48952-B0C4-E711-80C2-0003FF53067B' -- order gen event

)

, add_keys as (

    select
        md5(loyalty_points.loyalty_points_id) as pk_fact_loyalty_points
        , loyalty_points.loyalty_points_id
        , loyalty_points.loyalty_points_parent_id
        , loyalty_points.billing_agreement_id
        , loyalty_points.loyalty_event_id
        , loyalty_points.loyalty_order_id
        , loyalty_points.transaction_reason
        , loyalty_points.transaction_points
        , loyalty_points.remaining_points
        , loyalty_points.billing_agreement_order_id
        , agreements.company_id
        , loyalty_points.source_created_at
        , loyalty_points.points_expiration_date

        , points_from_multipliers.level_multiplier
        , points_from_multipliers.level_booster_points
        , agreements.loyalty_level_number
        
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
        , md5(loyalty_points.loyalty_event_id) as fk_dim_loyalty_events
        , cast(date_format(loyalty_points.source_created_at, 'yyyyMMdd') as int) as fk_dim_dates_transaction
        , cast(date_format(loyalty_points.points_expiration_date, 'yyyyMMdd') as int) as fk_dim_dates_points_expiration
        , md5(agreements.company_id) as fk_dim_companies
        , md5(concat(loyalty_seasons.company_id,loyalty_seasons.loyalty_season_start_date)) as fk_dim_loyalty_seasons
    from loyalty_points

    left join agreements 
        on loyalty_points.billing_agreement_id = agreements.billing_agreement_id
        and loyalty_points.source_created_at >= agreements.valid_from
        and loyalty_points.source_created_at < agreements.valid_to

    left join billing_agreement_preferences
        on agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id

    left join points_from_multipliers
        on md5(loyalty_points.loyalty_points_id) = points_from_multipliers.pk_fact_loyalty_points

    left join loyalty_seasons
        on agreements.company_id = loyalty_seasons.company_id 
        and loyalty_points.source_created_at >= loyalty_seasons.loyalty_season_start_date
        and loyalty_points.source_created_at < loyalty_seasons.loyalty_season_end_date

)

select * from add_keys
