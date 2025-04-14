with recipe_in_menu as (
    select distinct
        fk_dim_recipes,
        menu_year,
        menu_week,
        fk_dim_products
    from {env}.gold.fact_menus
    where (menu_year * 100 + menu_week) >= {start_yyyyww}
        and company_id = '{company_id}'
        and is_dish
),

main_recipe_ids as (
    select distinct
        pk_dim_recipes,
        main_recipe_id,
        recipe_name
    from {env}.gold.dim_recipes
),

product_id_mappings as (
    select
        pk_dim_products,
        product_id
    from {env}.gold.dim_products
)

select distinct
    menu_year,
    menu_week,
    menu_year * 100 + menu_week as menu_yyyyww,
    main_recipe_id,
    product_id
from recipe_in_menu
inner join main_recipe_ids
    on main_recipe_ids.pk_dim_recipes = recipe_in_menu.fk_dim_recipes
left join product_id_mappings
    on product_id_mappings.pk_dim_products = recipe_in_menu.fk_dim_products
order by menu_year, menu_week
