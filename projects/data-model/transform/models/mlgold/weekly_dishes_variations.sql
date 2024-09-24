with 

fact_menus as (
    select
        menu_year,
        menu_week,
        company_id,
        weekly_menu_id,
        menu_id,
        product_variation_id,
        menu_recipe_id,
        recipe_id,
        recipe_portion_id,
        portion_id,
        portion_size,
        menu_recipe_order,
        menu_number_days
    from
        {{ ref('fact_menus') }}
    -- only include menu variations with recipe_id and portion_id
    where has_menu_recipes is true
    and has_recipe_portions is true
)

, dim_products as (
    select
        product_type_id,
        product_id,
        product_variation_id,
        product_variation_name,
        company_id
    from
        {{ ref('dim_products') }}
)

, joined as (
    select
        fact_menus.*,
        dim_products.product_type_id,
        dim_products.product_variation_name
    from
        fact_menus
    left join dim_products
        on fact_menus.product_variation_id = dim_products.product_variation_id
        and fact_menus.company_id = dim_products.company_id
    where dim_products.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- dishes
    and fact_menus.recipe_id is not null
    order by menu_year, menu_week, product_variation_name
)

select * from joined
