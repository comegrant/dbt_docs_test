with 

weekly_menus as (

    select * from {{ ref('sil_pim__weekly_menus') }}

),

menus as (

    select * from {{ ref('sil_pim__menus') }}

),

menu_variations as (

    select * from {{ ref('sil_pim__menu_variations') }}

),

menu_recipes as (

    select * from {{ ref('sil_pim__menu_recipes') }}

),

recipes as (

    select * from {{ ref('sil_pim__recipes') }}

),

recipe_companies as (

    select * from {{ ref('sil_pim__recipe_companies') }}

),

recipe_portions as (

    select * from {{ ref('sil_pim__recipe_portions') }}

),

portions as (

    select * from {{ ref('sil_pim__portions') }}

),


companies as (

    select * from {{ ref('dim_companies') }}

),


weekly_menu_tables_joined as (
    select
        weekly_menus.company_id as weekly_menu_company_id
        , recipe_companies.company_id as recipe_company_id
        , weekly_menus.weekly_menu_id
        , menus.menu_id
        , menu_variations.product_variation_id
        , menu_recipes.recipe_id
        , recipes.recipe_metadata_id
        , recipe_portions.portion_id
        , companies.language_id
        
        , weekly_menus.menu_year
        , weekly_menus.menu_week
        , recipes.recipes_year
        , recipes.recipes_week

        , menu_variations.menu_number_days
        , variation_portions.portion_size as variation_portion_size
        , menu_variations.menu_price
        , menu_variations.menu_cost

        , menu_recipes.menu_recipe_order
        , portions.portion_size as recipe_portion_size

        {# Status #}
        , weekly_menus.weekly_menu_status_code_id
        , menus.menu_status_code_id
        , recipes.recipe_status_code_id

        {# FKS #}
        , md5(cast(concat(recipes.recipe_metadata_id, companies.language_id) as string)) as fk_dim_recipes
        , md5(menu_variations.product_variation_id) as fk_dim_products

    from weekly_menus
    left join menus 
        on weekly_menus.weekly_menu_id = menus.weekly_menu_id
    left join menu_variations 
        on menus.menu_id = menu_variations.menu_id
    left join portions as variation_portions
        on menu_variations.portion_id = variation_portions.portion_id
    left join menu_recipes
        on menus.menu_id = menu_recipes.menu_id
    left join recipes
        on menu_recipes.recipe_id = recipes.recipe_id
    left join recipe_companies
        on recipes.recipe_id = recipe_companies.recipe_id
    left join recipe_portions
        on recipes.recipe_id = recipe_portions.recipe_id
    left join portions
        on recipe_portions.portion_id = recipe_portions.portion_id
    left join companies
        on recipe_companies.company_id = companies.company_id

)

select * from weekly_menu_tables_joined