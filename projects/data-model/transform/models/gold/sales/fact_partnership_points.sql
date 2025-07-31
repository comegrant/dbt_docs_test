with

billing_agreement_partnership_loyalty_points as (

    select * from {{ ref('partnership__billing_agreement_partnership_loyalty_points') }}

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, add_keys as (

    select
        --pk
        md5(billing_agreement_partnership_loyalty_points.billing_agreement_partnership_loyalty_point_id) as pk_fact_partnership_points

        -- ids
        , billing_agreement_partnership_loyalty_points.billing_agreement_partnership_loyalty_point_id
        , billing_agreement_partnership_loyalty_points.billing_agreement_id
        , billing_agreement_partnership_loyalty_points.billing_agreement_order_id
        , billing_agreement_partnership_loyalty_points.company_partnership_id
        , billing_agreement_partnership_loyalty_points.partnership_rule_id

        -- cols
        , billing_agreement_partnership_loyalty_points.transaction_points
        , billing_agreement_partnership_loyalty_points.source_created_at
        

        --fks
        , md5(
            concat(
                billing_agreement_partnership_loyalty_points.company_partnership_id
                , billing_agreement_partnership_loyalty_points.partnership_rule_id
            )
        ) as fk_dim_partnerships
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , cast(date_format(billing_agreement_partnership_loyalty_points.source_created_at, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(agreements.company_id) as fk_dim_companies

    from billing_agreement_partnership_loyalty_points

    left join agreements
        on billing_agreement_partnership_loyalty_points.billing_agreement_id = agreements.billing_agreement_id
        and billing_agreement_partnership_loyalty_points.source_created_at >= agreements.valid_from
        and billing_agreement_partnership_loyalty_points.source_created_at < agreements.valid_to

)

select * from add_keys
