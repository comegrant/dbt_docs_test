with 

orders as (

    select * from {{ ref('sil_cms__billing_agreement_orders') }}

),

order_lines as (

    select * from {{ ref('sil_cms__billing_agreement_order_lines') }}

),

has_delivery as (

    select * from {{ ref('int_order_has_delivery') }}

),

billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

),

source_tables_joined as (
    select

        {# pk #}
        md5(order_lines.order_line_id) as pk_fact_order_line

        {# ids #}
        , orders.cms_order_id
        , orders.ops_order_id
        , orders.order_status_id
        , orders.order_type_id
        , order_lines.order_line_id
        , orders.agreement_id
        , order_lines.product_variation_id

        {# strings #}
        , order_lines.order_line_type_name

        {# numerics #}
        , order_lines.variation_qty
        , order_lines.vat
        , order_lines.unit_price_ex_vat
        , order_lines.unit_price_inc_vat
        , order_lines.total_amount_ex_vat
        , order_lines.total_amount_inc_vat

        {# booleans #}
        , orders.has_recipe_leaflets
        , coalesce(has_delivery.has_delivery, false) as has_delivery

        {# dates #}
        , orders.delivery_week_monday_date

        {# timestamp #}
        , orders.order_created_at

        {# foregin keys #}
        , md5(orders.order_status_id) AS fk_dim_order_status
        , md5(orders.order_type_id) AS fk_dim_order_types
        , md5(billing_agreements.company_id) AS fk_dim_companies
        , billing_agreements.pk_dim_billing_agreements AS fk_dim_billing_agreements
        , md5(
            concat(
                order_lines.product_variation_id,
                billing_agreements.company_id)
            )
        as fk_dim_products
        , cast(date_format(delivery_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date

    from order_lines
    left join orders
        on order_lines.cms_order_id = orders.cms_order_id
    left join has_delivery
        on order_lines.cms_order_id = has_delivery.cms_order_id
    left join billing_agreements
        on orders.agreement_id = billing_agreements.agreement_id


)

select * from source_tables_joined