with dim_companies as (
    select
        pk_dim_companies
        , company_id
        , language_id
    from {{ ref('dim_companies') }}
)

, recipes_in_menu as (
    select distinct
        fk_dim_companies
        , fk_dim_recipes
        , fk_dim_ingredient_combinations
        , recipe_portion_id
        , menu_id
        , fk_dim_portions
    from {{ ref('fact_menus') }}
    where
        (menu_year * 100 + menu_week) >= 202401
        and is_dish is true
)

, dim_recipes as (
    select distinct
        pk_dim_recipes
        , main_recipe_id
        , recipe_id
        , recipe_name
        , recipe_main_ingredient_name_english
        , (cooking_time_to + cooking_time_from) / 2 as cooking_time_mean
        , recipe_difficulty_level_id
    from {{ ref('dim_recipes') }}
    where recipe_main_ingredient_name_english is not null
)

, dim_portions as (
    select
        pk_dim_portions
        , portion_id
        , portions
    from {{ ref('dim_portions') }}
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

, number_of_recipe_steps as (
    select
        recipe_step_sections.recipe_portion_id
        , size(collect_set(recipe_steps.recipe_step_id)) as number_of_recipe_steps
    from recipe_step_sections
    left join recipe_steps
        on recipe_step_sections.recipe_step_section_id = recipe_steps.recipe_step_section_id
    group by 1
)

, recipes_taxonomies as (
    select * from {{ ref('pim__recipe_taxonomies') }}
)

, recipe_ingredient_combinations as (
    select
        pk_dim_ingredient_combinations
        , array_size(ingredient_id_list_array) as number_of_ingredients
    from {{ ref('dim_ingredient_combinations') }}
)

, taxonomies as (
    select taxonomy_id
    from {{ ref('pim__taxonomies') }}
    where taxonomy_status_code_id = 1 --active
)

, taxonomies_translations as (
    select
        *
        , lower(taxonomy_name) as taxonomy_name_lowercase
    from {{ ref('pim__taxonomy_translations') }}
)

, taxonomies_translations_with_flags as (
    select
        *
        , cast(
            taxonomy_name_lowercase like '%inspirasjon%'
            or taxonomy_name_lowercase like 'inspirerende'
            or taxonomy_name_lowercase like 'favoritter'
            or taxonomy_name_lowercase like 'chefs choice'
            or taxonomy_name_lowercase like 'cockens val'
            or taxonomy_name_lowercase like '%inspirerande%'
            as int
        ) as has_chefs_favorite_taxonomy
        , cast(
            taxonomy_name_lowercase like '%ekspress%'
            or taxonomy_name_lowercase like 'rask'
            or taxonomy_name_lowercase like 'laget på 1-2-3'
            or taxonomy_name_lowercase like 'fort gjort'
            or taxonomy_name_lowercase like 'snabb%'
            or taxonomy_name_lowercase like 'enkelt'
            or taxonomy_name_lowercase like 'hurtig%'
            or taxonomy_name_lowercase like 'nem på 5'
            as int
        ) as has_quick_and_easy_taxonomy
        , cast(
            taxonomy_name_lowercase like 'vegetar%'
            or taxonomy_name_lowercase like 'vegan'
            as int
        ) as has_vegetarian_taxonomy
        , cast(
            taxonomy_name_lowercase like 'low calorie'
            or taxonomy_name_lowercase like 'sunn%'
            or taxonomy_name_lowercase like '%sunt%'
            or taxonomy_name_lowercase like 'roede'
            or taxonomy_name_lowercase like 'kalorismart'
            or taxonomy_name_lowercase like '%viktväktarna%'
            or taxonomy_name_lowercase like '%sund%'
            or taxonomy_name_lowercase like '%kalorilet%'
            as int
        ) as has_low_calorie_taxonomy
    from taxonomies_translations
)

, taxonomies_list as (
    select
        recipes_taxonomies.recipe_id
        , concat_ws(', ', collect_list(taxonomies_translations_with_flags.taxonomy_name)) as taxonomy_list
        , size(
            collect_set(taxonomies_translations_with_flags.taxonomy_name)
        )                                                                                 as number_of_taxonomies
        , cast(sum(taxonomies_translations_with_flags.has_chefs_favorite_taxonomy)
        > 0 as int)                                                                               as has_chefs_favorite_taxonomy
        , cast(sum(taxonomies_translations_with_flags.has_quick_and_easy_taxonomy)
        > 0 as int)                                                                               as has_quick_and_easy_taxonomy
        , cast(sum(taxonomies_translations_with_flags.has_vegetarian_taxonomy)
        > 0 as int)                                                                               as has_vegetarian_taxonomy
        , cast(sum(taxonomies_translations_with_flags.has_low_calorie_taxonomy)
        > 0 as int)                                                                               as has_low_calorie_taxonomy
    from recipes_taxonomies
    left join taxonomies
        on recipes_taxonomies.taxonomy_id = taxonomies.taxonomy_id
    left join taxonomies_translations_with_flags
        on recipes_taxonomies.taxonomy_id = taxonomies_translations_with_flags.taxonomy_id
    where taxonomies_translations_with_flags.language_id <> 4 -- exclude English
    group by 1
)


, recipe_preferences_unioned as (
    select * from intermediate.int_recipe_preferences_unioned
)

, dim_preference_combinations as (
    select * from {{ ref('dim_preference_combinations') }}
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

, final as (
    select distinct
        dim_companies.company_id
        , dim_recipes.main_recipe_id
        , dim_recipes.recipe_name
        , dim_recipes.recipe_main_ingredient_name_english
        , dim_recipes.cooking_time_mean
        , dim_recipes.recipe_difficulty_level_id
        , number_of_recipe_steps.number_of_recipe_steps
        , taxonomies_list.number_of_taxonomies
        , recipe_ingredient_combinations.number_of_ingredients
        , taxonomies_list.has_chefs_favorite_taxonomy
        , taxonomies_list.has_quick_and_easy_taxonomy
        , taxonomies_list.has_vegetarian_taxonomy
        , taxonomies_list.has_low_calorie_taxonomy
        , recipe_allergies_list.allergen_preference_id_list
        , recipe_allergies_list.allergen_name_combinations
    from recipes_in_menu
    left join dim_recipes
        on recipes_in_menu.fk_dim_recipes = dim_recipes.pk_dim_recipes
    left join dim_companies
        on recipes_in_menu.fk_dim_companies = dim_companies.pk_dim_companies
    left join dim_portions
        on recipes_in_menu.fk_dim_portions = dim_portions.pk_dim_portions
    left join number_of_recipe_steps
        on recipes_in_menu.recipe_portion_id = number_of_recipe_steps.recipe_portion_id
    left join recipe_ingredient_combinations
        on recipes_in_menu.fk_dim_ingredient_combinations = recipe_ingredient_combinations.pk_dim_ingredient_combinations
    left join taxonomies_list
        on dim_recipes.recipe_id = taxonomies_list.recipe_id
    left join recipe_allergies_list
        on dim_recipes.recipe_id = recipe_allergies_list.recipe_id
    where portions = 4
    and main_recipe_id != 0
)

select * from final
