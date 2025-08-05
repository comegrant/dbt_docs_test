with recipe_in_menu as (
    select distinct
        fk_dim_recipes
        , menu_year
        , menu_week
        , fk_dim_products
        , company_id
    from {{ ref('fact_menus') }}
    where
        (menu_year * 100 + menu_week) >= 202101
        and is_dish
)

, main_recipe_ids as (
    select distinct
        pk_dim_recipes
        , main_recipe_id
        , recipe_name
    from {{ ref('dim_recipes') }}
    where recipe_name is not null
)

, product_id_mappings as (
    select
        pk_dim_products
        , product_id
    from {{ ref('dim_products') }}
)

, final as (
    select distinct
        company_id
        ,menu_year
        , menu_week
        , menu_year * 100 + menu_week as menu_yyyyww
        , main_recipe_id
        , product_id
    from recipe_in_menu
    inner join main_recipe_ids
        on recipe_in_menu.fk_dim_recipes = main_recipe_ids.pk_dim_recipes
    left join product_id_mappings
        on recipe_in_menu.fk_dim_products = product_id_mappings.pk_dim_products
    where main_recipe_id != 0
    order by menu_year, menu_week

)

select * from final
