with

fact_menus as (
    select
        fk_dim_companies
        , fk_dim_recipes
        , fk_dim_portions
        , menu_id
        , recipe_portion_id
    from {{ ref('fact_menus') }}
    -- only include menu variations with recipe_id and portion_id
    where
        is_dish is true
        and menu_recipe_id is not null
        and fk_dim_dates >= 20190101
)

, dim_portions as (
    select
        pk_dim_portions
        , portion_id
        , portions
    from {{ ref('dim_portions') }}
)

, dim_companies as (
    select
        pk_dim_companies
        , company_id
        , language_id
    from {{ ref('dim_companies') }}
)

, dim_recipes as (
    select
        pk_dim_recipes
        , main_recipe_id
        , recipe_id
        , recipe_name
        , cooking_time_from
        , cooking_time_to
        , recipe_difficulty_level_id
        , recipe_difficulty_name
        , recipe_main_ingredient_id
        , recipe_main_ingredient_name_local
    from {{ ref('dim_recipes') }}
)

, dim_preference_combinations as (
    select * from {{ ref('dim_preference_combinations') }}
)

, recipe_preferences_unioned as (
    select * from {{ ref('int_recipe_preferences_unioned') }}
)

, menu_products as (
    select
        menu_id
        , product_id
    from {{ ref('pim__menus') }}
)

, products as (
    select
        product_id
        , product_type_id
    from {{ ref('product_layer__products') }}
)

, recipes_taxonomies as (
    select * from {{ ref('pim__recipe_taxonomies') }}
)

, taxonomies as (
    select taxonomy_id
    from {{ ref('pim__taxonomies') }}
    where taxonomy_status_code_id = 1 --active
)

, taxonomies_translations as (
    select * from {{ ref('pim__taxonomy_translations') }}
)

, recipe_portions as (
    select
        recipe_id
        , recipe_portion_id
    from {{ ref('pim__recipe_portions') }}
)

, chef_ingredient_sections as (
    select
        chef_ingredient_section_id
        , recipe_portion_id
    from {{ ref('pim__chef_ingredient_sections') }}
)

, generic_ingredients as (
    select
        generic_ingredient_id
        , chef_ingredient_section_id
    from {{ ref('pim__chef_ingredients') }}
)

, recipe_step_sections as (
    select
        recipe_step_section_id
        , recipe_portion_id
    from {{ ref('pim__recipe_step_sections') }}
)

, recipe_steps as (
    select
        recipe_step_id
        , recipe_step_section_id
    from {{ ref('pim__recipe_steps') }}
)

, taxonomies_list as (
    select
        recipes_taxonomies.recipe_id
        , concat_ws(', ', collect_list(taxonomies_translations.taxonomy_name)) as taxonomy_list
        , size(collect_set(taxonomies_translations.taxonomy_name))             as number_of_taxonomies
    from recipes_taxonomies
    left join taxonomies
        on recipes_taxonomies.taxonomy_id = taxonomies.taxonomy_id
    left join taxonomies_translations
        on recipes_taxonomies.taxonomy_id = taxonomies_translations.taxonomy_id
    where taxonomies_translations.language_id <> 4 -- exclude English
    group by 1
)

, generic_ingredients_list as (
    select
        portions.recipe_id
        , concat_ws(', ', collect_set(ingredients.generic_ingredient_id)) as generic_ingredient_id_list
        , size(collect_set(ingredients.generic_ingredient_id))            as number_of_ingredients
    from recipe_portions as portions
    left join chef_ingredient_sections as sections
        on portions.recipe_portion_id = sections.recipe_portion_id
    left join generic_ingredients as ingredients
        on
            sections.chef_ingredient_section_id
            = ingredients.chef_ingredient_section_id
    where ingredients.generic_ingredient_id is not null
    group by 1
)

, recipe_steps_list as (
    select
        step_sections.recipe_portion_id
        , concat_ws(', ', collect_list(steps.recipe_step_id)) as recipe_step_id_list
        , size(collect_set(steps.recipe_step_id))             as number_of_recipe_steps
    from recipe_step_sections as step_sections
    left join recipe_steps as steps
        on step_sections.recipe_step_section_id = steps.recipe_step_section_id
    group by 1

)

, recipe_allergies_list as (
    select
        dim_recipes.recipe_id
        , dim_preference_combinations.allergen_preference_id_list
        , dim_preference_combinations.allergen_name_combinations
    from dim_recipes
    left join recipe_preferences_unioned
        on dim_recipes.recipe_id = recipe_preferences_unioned.recipe_id
    left join dim_preference_combinations
        on
            recipe_preferences_unioned.preference_combination_id
            = dim_preference_combinations.pk_dim_preference_combinations
)

, excluded_recipes as (
    select recipe_id
    from recipes_taxonomies
    where taxonomy_id = 1572 -- Specialvarer
)

, distinct_recipes as (
    select distinct
        fact_menus.fk_dim_companies
        , fact_menus.fk_dim_recipes
        , dim_companies.company_id
        , dim_companies.language_id
        , dim_recipes.main_recipe_id
        , dim_recipes.recipe_id
        , fact_menus.recipe_portion_id
        , dim_recipes.recipe_name
        , dim_recipes.cooking_time_from
        , dim_recipes.cooking_time_to
        , dim_recipes.recipe_difficulty_level_id
        , dim_recipes.recipe_difficulty_name
        , dim_recipes.recipe_main_ingredient_id
        , dim_recipes.recipe_main_ingredient_name_local
        , taxonomies_list.taxonomy_list
        , taxonomies_list.number_of_taxonomies
        , generic_ingredients_list.generic_ingredient_id_list
        , generic_ingredients_list.number_of_ingredients
        , recipe_steps_list.recipe_step_id_list
        , recipe_steps_list.number_of_recipe_steps
        , recipe_allergies_list.allergen_preference_id_list
        , recipe_allergies_list.allergen_name_combinations
    from fact_menus
    left join dim_companies
        on fact_menus.fk_dim_companies = dim_companies.pk_dim_companies
    left join dim_recipes
        on fact_menus.fk_dim_recipes = dim_recipes.pk_dim_recipes
    left join taxonomies_list
        on dim_recipes.recipe_id = taxonomies_list.recipe_id
    left join generic_ingredients_list
        on dim_recipes.recipe_id = generic_ingredients_list.recipe_id
    left join recipe_allergies_list
        on dim_recipes.recipe_id = recipe_allergies_list.recipe_id
    left join recipe_steps_list
        on fact_menus.recipe_portion_id = recipe_steps_list.recipe_portion_id
    left join menu_products
        on fact_menus.menu_id = menu_products.menu_id
    left join products
        on menu_products.product_id = products.product_id
    left join dim_portions
        on fact_menus.fk_dim_portions = dim_portions.pk_dim_portions
    where
        products.product_type_id in (
            'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- single dishes
            , '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- mealboxes
        )
        and dim_portions.portions = 4
        and dim_portions.portion_id <> 15 -- plus portions for Retnemt
        and dim_companies.company_id in (
            '5E65A955-7B1A-446C-B24F-CFE576BF52D7' -- Retnemt
            , '8A613C15-35E4-471F-91CC-972F933331D7' -- Adams Matkasse
            , '09ECD4F0-AE58-4539-8E8F-9275B1859A19' -- Godtlevert
            , '6A2D0B60-84D6-4830-9945-58D518D27AC2' -- Linas Matkasse
        )
        and dim_recipes.recipe_id not in (
            select recipe_id from excluded_recipes
        )
        and dim_recipes.recipe_id not in (11434, 12537)
)

select * from distinct_recipes
