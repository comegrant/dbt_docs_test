with fact_orders as (
    select * from gold.fact_orders
)

, dim_products as (
    select * from gold.dim_products
)

, dim_companies as (
    select
        pk_dim_companies,
        company_name
    from gold.dim_companies
)

, fact_orders_joined_product_type_joined_companies as (
    select
        fact_orders.*
        , product_type_id
        , product_type_name
        , company_name
    from fact_orders
    left join dim_products
        on fact_orders.fk_dim_products = dim_products.pk_dim_products
    left join dim_companies
        on fact_orders.fk_dim_companies = dim_companies.pk_dim_companies
    where
        has_finished_order_status
        and product_type_id in (
            '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- mealbox
            , '288ED8CA-B437-4A6D-BE16-8F9A30185008' -- legacy financial
        )
        and fact_orders.company_id in (
            '8A613C15-35E4-471F-91CC-972F933331D7' -- AMK
            , '09ECD4F0-AE58-4539-8E8F-9275B1859A19' -- GL
            , '6A2D0B60-84D6-4830-9945-58D518D27AC2' -- LMK
            , '5E65A955-7B1A-446C-B24F-CFE576BF52D7' --RT
        )
)

, aggregated_order as (
    select
        menu_year
        , menu_week
        , company_id
        , company_name
        , sum(product_variation_quantity) as total_orders
    from fact_orders_joined_product_type_joined_companies
    group by
        menu_year
        , menu_week
        , company_id
        , company_name
    order by menu_year asc, menu_week asc
)


select
    *
from aggregated_order
