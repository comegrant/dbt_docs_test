with 

loyalty_order_lines as (

    select * from {{ ref('int_loyalty_order_lines_joined') }}

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, loyalty_seasons as (

    select * from {{ ref('dim_loyalty_seasons') }}

)

, billing_agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}
  
)

, add_pk as (

    select         
        md5(concat(
            loyalty_order_lines.loyalty_order_id,
            loyalty_order_lines.loyalty_order_line_id
            )
            ) as pk_fact_loyalty_orders,
        *
    from loyalty_order_lines

)

, add_fks as (

    select 
        add_pk.pk_fact_loyalty_orders
        , add_pk.product_variation_quantity
        , add_pk.unit_point_price
        , add_pk.total_point_price
        , add_pk.loyalty_order_id
        , add_pk.loyalty_order_line_id
        , add_pk.loyalty_order_status_id
        , add_pk.order_week_monday_date
        , add_pk.order_year
        , add_pk.order_week
        , add_pk.billing_agreement_id
        , agreements.company_id
        , add_pk.product_variation_id
        , md5(cast(add_pk.loyalty_order_status_id as string)) AS fk_dim_loyalty_order_statuses
        , cast(date_format(add_pk.order_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
        , md5(agreements.company_id) AS fk_dim_companies
        , md5(
            concat(
                add_pk.product_variation_id,
                agreements.company_id)
            ) as fk_dim_products
        , md5(
            concat(
                loyalty_seasons.company_id,
                loyalty_seasons.loyalty_season_start_date)
            ) as fk_dim_loyalty_seasons
    from add_pk
    left join agreements 
        on add_pk.billing_agreement_id = agreements.billing_agreement_id  
        and add_pk.loyalty_order_created_at >= agreements.valid_from 
        and add_pk.loyalty_order_created_at < agreements.valid_to
    left join billing_agreement_preferences
        on agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id
    left join loyalty_seasons 
        on agreements.company_id = loyalty_seasons.company_id
        and add_pk.loyalty_order_created_at >= loyalty_seasons.loyalty_season_start_date
        and add_pk.loyalty_order_created_at < loyalty_seasons.loyalty_season_end_date
)


select * from add_fks