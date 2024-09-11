with weekly_menu as (
    select
        menu_year as delivery_year,
        menu_week as delivery_week,
        company_id,
        weekly_menu_id
    from
        {{ ref('pim__weekly_menus') }}
),

fact_menus as (
    select
        weekly_menu_id,
        menu_id,
        recipe_id,
        portion_id,
        recipe_portion_size,
        fk_dim_companies
    from
        {{ ref('fact_menus') }}
),

dim_companies as (
    select
        pk_dim_companies,
        company_id
    from
        {{ ref('dim_companies') }}
),

fact_menus_with_company as (
    select
        fact_menus.*,
        dim_companies.company_id
    from
        fact_menus
    left join
        dim_companies
        on fact_menus.fk_dim_companies = dim_companies.pk_dim_companies
),

{# another intermedaite table #}
weekly_menus_joined as (
    select
        weekly_menu.*,
        fact_menus_with_company.menu_id,
        fact_menus_with_company.recipe_id,
        fact_menus_with_company.portion_id,
        fact_menus_with_company.recipe_portion_size
    from weekly_menu
    left join
        fact_menus_with_company
        on
            weekly_menu.company_id = fact_menus_with_company.company_id
            and weekly_menu.weekly_menu_id
            = fact_menus_with_company.weekly_menu_id
),

menu_recipes as (
    select
        menu_recipe_id,
        menu_id,
        recipe_id,
        menu_recipe_order
    from {{ ref('pim__menu_recipes') }}
),

recipe_portions as (
    select
        recipe_portion_id,
        recipe_id,
        portion_id
    from {{ ref("pim__recipe_portions") }}
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
        on variation_product.product_id = product_type.product_id
),

variation_name as (
    select
        product_variation_id,
        product_variation_name,
        company_id
    from {{ ref('product_layer__product_variations_companies') }}
),

joined as (
    select
        weekly_menus_joined.*,
        menu_recipe_order,
        menu_variations.product_variation_id,
        recipe_portions.recipe_portion_id,
        menu_variations.menu_number_days,
        product_type_id,
        product_variation_name
    from
        weekly_menus_joined
    left join
        menu_recipes
        on weekly_menus_joined.menu_id = menu_recipes.menu_id
    left join
        menu_variations
        on
            weekly_menus_joined.menu_id = menu_variations.menu_id
            and weekly_menus_joined.portion_id = menu_variations.portion_id
    left join
        recipe_portions
        on
            weekly_menus_joined.recipe_id = recipe_portions.recipe_id
            and weekly_menus_joined.portion_id = recipe_portions.portion_id
    left join variation_name
        on
            menu_variations.product_variation_id
            = variation_name.product_variation_id
            and weekly_menus_joined.company_id = variation_name.company_id
    left join variations_product_type
        on
            menu_variations.product_variation_id
            = variations_product_type.product_variation_id
    where product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- dishes
    order by delivery_year, delivery_week, product_variation_name
)

select * from joined
