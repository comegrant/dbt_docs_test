with 

orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}

)

, order_lines as (

    select * from {{ ref('cms__billing_agreement_order_lines') }}

)

, has_delivery as (

    select * from {{ ref('int_billing_agreement_order_lines_with_delivery') }}

)

, recipes as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, companies as (

       select * from {{ ref('dim_companies') }}

)

, all_tables_joined as (
    select

        md5(order_lines.order_line_id) as pk_fact_orders
        
        , orders.menu_year
        , orders.menu_week
        , orders.menu_week_monday_date
        , orders.source_created_at
        , order_lines.product_variation_quantity
        , order_lines.vat
        , order_lines.unit_price_ex_vat
        , order_lines.unit_price_inc_vat
        , order_lines.total_amount_ex_vat
        , order_lines.total_amount_inc_vat
        , order_lines.order_line_type_name
        , orders.has_recipe_leaflets
        , coalesce(has_delivery.has_delivery, false) as has_delivery

        , billing_agreements.company_id
        , orders.cms_order_id
        , orders.ops_order_id
        , orders.order_status_id
        , orders.order_type_id
        , order_lines.order_line_id
        , orders.billing_agreement_id
        , order_lines.product_variation_id
        
        , billing_agreements.pk_dim_billing_agreements AS fk_dim_billing_agreements
        , md5(companies.company_id) AS fk_dim_companies
        , cast(date_format(orders.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
        , md5(orders.order_status_id) AS fk_dim_order_statuses
        , md5(orders.order_type_id) AS fk_dim_order_types
        , md5(
            concat(
                order_lines.product_variation_id,
                billing_agreements.company_id
                )
            ) as fk_dim_products
    
    from order_lines
    left join orders
        on order_lines.cms_order_id = orders.cms_order_id
    left join has_delivery
        on order_lines.cms_order_id = has_delivery.cms_order_id
    left join billing_agreements
        on orders.billing_agreement_id = billing_agreements.billing_agreement_id
        and orders.source_created_at >= billing_agreements.valid_from
        and orders.source_created_at < billing_agreements.valid_to
    left join companies
        on billing_agreements.company_id = companies.company_id
)

select * from all_tables_joined