with 

loyalty_order_lines as (

    select * from {{ ref('int_loyalty_order_lines_joined') }}

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

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
        , md5(agreements.company_id) AS fk_dim_companies
        , md5(
            concat(
                add_pk.product_variation_id,
                agreements.company_id)
            ) as fk_dim_products
    from add_pk
    left join agreements 
        on add_pk.billing_agreement_id = agreements.billing_agreement_id  
        and add_pk.loyalty_order_created_at >= agreements.valid_from 
        and add_pk.loyalty_order_created_at < agreements.valid_to
)


select * from add_fks