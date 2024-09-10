with fact_menu as (
    select
        fk_dim_companies,
        fk_dim_recipes,
        menu_id,
        recipe_portion_size
    from {{ ref('fact_menus') }}
),

dim_companies as (
    select
        pk_dim_companies,
        company_id
    from {{ ref('dim_companies') }}
),

dim_recipes as (
    select
        pk_dim_recipes,
        recipe_id,
        recipe_name,
        cooking_time_from,
        cooking_time_to,
        recipe_difficulty_level_id,
        recipe_difficulty_name,
        recipe_main_ingredient_id,
        recipe_main_ingredient_name
    from {{ ref('dim_recipes') }}
),

menu_products as (
    select
        menu_id,
        product_id
    from {{ ref('pim__menus') }}
),

products as (
    select
        product_id,
        product_type_id
    from {{ ref('product_layer__products') }}
),

recipes_taxonomies as (
    select * from {{ ref('pim__recipes_taxonomies') }}
),

taxonomies as (
    select taxonomy_id
    from {{ ref('pim__taxonomies') }}
    where status_code_id = 1 --active
),

taxonomies_translations as (
    select * from {{ ref('pim__taxonomies_translations') }}
),

recipe_portions as (
    select
        recipe_id,
        recipe_portion_id
    from {{ ref('pim__recipe_portions') }}
),

chef_ingredient_sections as (
    select
        chef_ingredient_section_id,
        recipe_portion_id
    from {{ ref('pim__chef_ingredient_sections') }}
),

chef_ingredients as (
    select
        chef_ingredient_id,
        chef_ingredient_section_id
    from {{ ref('pim__chef_ingredients') }}
),

taxonomies_list as (
    select
        recipes_taxonomies.recipe_id,
        concat_ws(', ', collect_list(taxonomies_translations.taxonomy_name))
            as taxonomy_list
    from taxonomies
    left join recipes_taxonomies
        on taxonomies.taxonomy_id = recipes_taxonomies.taxonomy_id
    left join taxonomies_translations
        on recipes_taxonomies.taxonomy_id = taxonomies_translations.taxonomy_id
    group by 1
),

chef_ingredients_list as (
    select
        portions.recipe_id,
        concat_ws(', ', collect_list(ingredients.chef_ingredient_id))
            as chef_ingredient_id_list
    from recipe_portions as portions
    left join chef_ingredient_sections as sections
        on portions.recipe_portion_id = sections.recipe_portion_id
    left join chef_ingredients as ingredients
        on
            sections.chef_ingredient_section_id
            = ingredients.chef_ingredient_section_id
    where ingredients.chef_ingredient_id is not null
    group by 1
),

distinct_recipes as (
    select distinct
        fact_menu.fk_dim_companies,
        fact_menu.fk_dim_recipes,
        dim_companies.company_id,
        dim_recipes.recipe_id,
        dim_recipes.recipe_name,
        dim_recipes.cooking_time_from,
        dim_recipes.cooking_time_to,
        dim_recipes.recipe_difficulty_level_id,
        dim_recipes.recipe_difficulty_name,
        dim_recipes.recipe_main_ingredient_id,
        dim_recipes.recipe_main_ingredient_name,
        taxonomies_list.taxonomy_list,
        chef_ingredients_list.chef_ingredient_id_list
    from fact_menu
    left join dim_companies
        on fact_menu.fk_dim_companies = dim_companies.pk_dim_companies
    left join dim_recipes
        on fact_menu.fk_dim_recipes = dim_recipes.pk_dim_recipes
    left join taxonomies_list
        on dim_recipes.recipe_id = taxonomies_list.recipe_id
    left join chef_ingredients_list
        on dim_recipes.recipe_id = chef_ingredients_list.recipe_id
    left join menu_products
        on fact_menu.menu_id = menu_products.menu_id
    left join products
        on menu_products.product_id = products.product_id
    where
        products.product_type_id in (
            'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', -- single dishes
            '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- mealboxes
        )
        and fact_menu.recipe_portion_size = 4
        and dim_companies.company_id in (
            '5E65A955-7B1A-446C-B24F-CFE576BF52D7', -- Retnemt
            '8A613C15-35E4-471F-91CC-972F933331D7', -- Adams Matkasse
            '09ECD4F0-AE58-4539-8E8F-9275B1859A19', -- Godtlevert
            '6A2D0B60-84D6-4830-9945-58D518D27AC2' -- Linas Matkasse
        )
)

select * from distinct_recipes
