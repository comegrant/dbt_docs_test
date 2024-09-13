with fact_orders as (
    select
        delivery_week_monday_date,
        extract(year from delivery_week_monday_date) as delivery_year,
        extract(week from delivery_week_monday_date) as delivery_week,
        product_variation_id,
        variation_qty,
        fk_dim_order_statuses,
        fk_dim_companies,
        fk_dim_billing_agreements,
        fk_dim_products
    from {{ ref('fact_orders') }}
),

dim_order_statuses as (
    select
        pk_dim_order_statuses,
        order_status_id
    from
        {{ ref('dim_order_statuses') }}
),

dim_companies as (
    select
        pk_dim_companies,
        company_id,
        company_name
    from
        {{ ref('dim_companies') }}
),

dim_products as (
    select
        pk_dim_products,
        product_type_id
    from
        {{ ref('dim_products') }}
),

finished_standalone_dishes_orders as (
    select
        fact_orders.delivery_year,
        fact_orders.delivery_week,
        fact_orders.delivery_week_monday_date,
        fact_orders.product_variation_id,
        fact_orders.variation_qty,
        dim_companies.company_id,
        dim_companies.company_name
    from
        fact_orders
    left join
        dim_products
        on dim_products.pk_dim_products = fact_orders.fk_dim_products
    left join
        dim_order_statuses
        on dim_order_statuses.pk_dim_order_statuses = fact_orders.fk_dim_order_statuses
    left join
        dim_companies
        on dim_companies.pk_dim_companies = fact_orders.fk_dim_companies
    where dim_products.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- Velg&vrak, or standalone dishes
        and dim_order_statuses.order_status_id = '4508130E-6BA1-4C14-94A4-A56B074BB135' -- Finished
),

per_variation_aggregated as (
    select
        company_id,
        company_name,
        delivery_year,
        delivery_week,
        delivery_week_monday_date,
        product_variation_id,
        sum(variation_qty) as variation_qty
    from
        finished_standalone_dishes_orders
    group by
        company_id,
        company_name,
        delivery_year,
        delivery_week,
        delivery_week_monday_date,
        product_variation_id
),

per_company_aggregated as (
    select
        company_id,
        delivery_year,
        delivery_week,
        sum(variation_qty) as total_weekly_qty
    from
        finished_standalone_dishes_orders
    group by
        company_id,
        delivery_year,
        delivery_week
),

per_variation_and_company_aggregated_joined as (
    select
        per_variation_aggregated.*,
        per_company_aggregated.total_weekly_qty,
        per_variation_aggregated.variation_qty / per_company_aggregated.total_weekly_qty as variation_ratio
    from
        per_variation_aggregated
    left join
        per_company_aggregated
    on
        per_company_aggregated.company_id = per_variation_aggregated.company_id
        and per_company_aggregated.delivery_year = per_variation_aggregated.delivery_year
        and per_company_aggregated.delivery_week = per_variation_aggregated.delivery_week
    order by per_variation_aggregated.delivery_year, per_variation_aggregated.delivery_week
)

select
    *
from per_variation_and_company_aggregated_joined
