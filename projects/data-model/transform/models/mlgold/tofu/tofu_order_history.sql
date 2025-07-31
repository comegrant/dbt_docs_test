with

fact_orders as (
    select * from {{ ref('fact_orders') }}
)

, dim_products as (
    select * from {{ ref('dim_products') }}
)

, fact_orders_joined_product_type as (
    select
        fact_orders.*
        , product_type_id
        , product_type_name
    from fact_orders
    left join dim_products
        on fact_orders.fk_dim_products = dim_products.pk_dim_products
    where
        has_finished_order_status
        and product_type_id in (
            '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- mealbox
            , '288ED8CA-B437-4A6D-BE16-8F9A30185008' -- legacy financial
        )
)

, aggregated_order as (
    select
        menu_year
        , menu_week
        , company_id
        , sum(product_variation_quantity) as total_orders
    from fact_orders_joined_product_type
    where
        product_type_id in (
            '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- mealbox
            , '288ED8CA-B437-4A6D-BE16-8F9A30185008'
        )
    group by
        menu_year
        , menu_week
        , company_id
    order by menu_year asc, menu_week asc
)

, flex_orders_post_onesub as (
    select
        menu_year
        , menu_week
        , company_id
        , count(distinct billing_agreement_order_id) as number_orders_with_flex
    from
        fact_orders_joined_product_type
    where
        (
            has_swap
            or is_removed_dish = 1
            or is_added_dish = 1
        )
        and (menu_year * 100 + menu_week) >= 202447 -- onesub launch
    group by menu_year, menu_week, company_id
    order by menu_year asc, menu_week asc
)


, flex_orders_prep_onesub as (
    select
        menu_year
        , menu_week
        , company_id
        , sum(product_variation_quantity) as total_orders_with_flex
    from fact_orders_joined_product_type
    where
        product_type_id in (
            '288ED8CA-B437-4A6D-BE16-8F9A30185008' -- legacy financial
        )
        and (menu_year * 100 + menu_week) <= 202446
    group by
        menu_year
        , menu_week
        , company_id
)

, flex_orders as (
    select *
    from flex_orders_prep_onesub
    union all
    select *
    from flex_orders_post_onesub
)

, final as (
    select
        aggregated_order.*
        , total_orders_with_flex
        , total_orders_with_flex / total_orders as flex_share
    from aggregated_order
    left join
        flex_orders
        on
            aggregated_order.menu_year = flex_orders.menu_year
            and aggregated_order.menu_week = flex_orders.menu_week
            and aggregated_order.company_id = flex_orders.company_id
    order by menu_year asc, menu_week asc, company_id asc
)

select * from final
