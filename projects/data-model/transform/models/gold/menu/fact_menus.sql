with 

menu_weeks as (

    select * from {{ ref('int_weekly_menus_variations_recipes_joined') }}

),

portions as (

    select * from {{ ref('pim__portions') }}

),


companies as (

    select * from {{ ref('dim_companies') }}

),

all_tables_joined as (
    select
        md5(concat_ws(
            menu_weeks.weekly_menu_id,
            menu_weeks.menu_id,
            menu_weeks.product_variation_id,
            menu_weeks.recipe_id,
            menu_weeks.portion_id_variations,
            menu_weeks.menu_number_days,
            menu_weeks.menu_recipe_order
        )) as pk_fact_menus
        , menu_weeks.weekly_menu_id
        , menu_weeks.company_id
        , menu_weeks.menu_id
        , menu_weeks.product_variation_id
        , menu_weeks.menu_recipe_id
        , menu_weeks.recipe_id
        , menu_weeks.recipe_portion_id
        , menu_weeks.portion_id_variations
        , menu_weeks.portion_id_recipes

        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , menu_weeks.menu_week_monday_date

        , menu_weeks.menu_number_days
        , menu_weeks.menu_recipe_order

        -- added temporariliy for debugging purposes
        , portion_variations.portion_size as variation_portion_size
        , portion_recipes.portion_size as recipe_portion_size

        , menu_weeks.weekly_menu_status_code_id
        , menu_weeks.menu_status_code_id
        , menu_weeks.recipe_status_code_id
        , portion_variations.portion_status_code_id as variation_portion_status_code_id
        , portion_recipes.portion_status_code_id as recipe_portion_status_code_id
        
        {# FKS #}
        , md5(cast(concat(menu_weeks.recipe_id, companies.language_id) as string)) as fk_dim_recipes
        , md5(concat(menu_weeks.product_variation_id, companies.company_id)) as fk_dim_products
        , cast(date_format(menu_weeks.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
        , md5(menu_weeks.company_id) as fk_dim_companies

    from menu_weeks
    left join companies
        on menu_weeks.company_id = companies.company_id
    -- added temporariliy for debugging purposes
    left join portions as portion_variations
        on menu_weeks.portion_id_variations = portion_variations.portion_id
    left join portions as portion_recipes
        on menu_weeks.portion_id_recipes = portion_recipes.portion_id
)

select * from all_tables_joined