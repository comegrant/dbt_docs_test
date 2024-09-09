with weekly_menu as (
    select
        menu_year as year,
        menu_week as week,
        company_id,
        weekly_menu_id
    from
        {{ref('pim__weekly_menus')}}
),

f_menus as (
    select
        weekly_menu_id,
        menu_id,
        recipe_id,
        portion_id,
        recipe_portion_size,
        fk_dim_companies
    from
        {{ ref('fact_menus')}}
),

d_companies as (
    select
        pk_dim_companies,
        company_id
    from
        {{ ref('dim_companies') }}
),

f_menus_with_company as (
    select
        f_menus.*,
        d_companies.company_id
    from
        f_menus
    left join
        d_companies
    on d_companies.pk_dim_companies = f_menus.fk_dim_companies
),

{# another intermedaite table #}
weekly_menus_joined as (
    select
        weekly_menu.*,
        f_menus_with_company.menu_id,
        f_menus_with_company.recipe_id,
        f_menus_with_company.portion_id,
        f_menus_with_company.recipe_portion_size
    from weekly_menu
    left join
        f_menus_with_company
    on f_menus_with_company.company_id = weekly_menu.company_id
    and f_menus_with_company.weekly_menu_id = weekly_menu.weekly_menu_id
),

menu_recipes as (
    select
        menu_recipe_id,
        menu_id,
        recipe_id,
        menu_recipe_order
    from {{ ref('pim__menu_recipes') }}
),

menu_variations as (
    select
        menu_id,
        product_variation_id,
        menu_number_days,
        portion_id
    from {{ ref('pim__menu_variations') }}
),

variation_product as (
    select
        product_variation_id,
        product_id
    from
    {{ ref('product_layer__product_variations') }}
),

product_type as (
    select
        product_id,
        product_type_id
    from
        {{ ref('product_layer__products') }}
),

{# another intermedaite table #}
variations_product_type as (
    select
        product_variation_id,
        product_type_id
    from
        variation_product
    left join
        product_type
    on product_type.product_id = variation_product.product_id
),

variation_name as (
    select
        product_variation_id,
        product_variation_name,
        company_id
    from {{ ref('product_layer__product_variations_companies') }}
)

select
    weekly_menus_joined.*,
    menu_recipe_order,
    menu_variations.product_variation_id,
    menu_variations.menu_number_days,
    product_type_id,
    product_variation_name
from
    weekly_menus_joined
left join
    menu_recipes
    on menu_recipes.menu_id = weekly_menus_joined.menu_id
left join
    menu_variations
    on menu_variations.menu_id = weekly_menus_joined.menu_id
    and menu_variations.portion_id = weekly_menus_joined.portion_id
left join variation_name
    on variation_name.product_variation_id = menu_variations.product_variation_id
    and variation_name.company_id = weekly_menus_joined.company_id
left join variations_product_type
    on variations_product_type.product_variation_id = menu_variations.product_variation_id
where product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- dishes
order by year, week, product_variation_name
