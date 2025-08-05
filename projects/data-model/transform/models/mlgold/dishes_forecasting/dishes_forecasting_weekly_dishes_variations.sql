with

fact_menus as (
    select
        menu_year
        , menu_week
        , company_id
        , language_id
        , product_variation_id
        , recipe_id
        , recipe_portion_id
        , fk_dim_products
        , fk_dim_portions
    from
        {{ ref('fact_menus') }}
    -- only include menu variations with recipe_id and portion_id
    where
        is_dish is true
)

, dim_products as (
    select
        pk_dim_products
        , product_type_id
        , product_id
        , product_variation_id
        , product_variation_name
    from
        {{ ref('dim_products') }}
)

, dim_portions as (
    select
        pk_dim_portions
        , portion_id
        , portions as portion_quantity
    from {{ ref('dim_portions') }}
)

, joined as (
    select
        fact_menus.*
        , dim_portions.portion_id
        , dim_portions.portion_quantity
        , dim_products.product_variation_name
    from
        fact_menus
    left join dim_products
        on fact_menus.fk_dim_products = dim_products.pk_dim_products
    left join dim_portions
        on fact_menus.fk_dim_portions = dim_portions.pk_dim_portions
    where fact_menus.recipe_id is not null
    order by fact_menus.menu_year, fact_menus.menu_week, dim_products.product_variation_name
)

select * from joined
