with fact_menus as (

    select * from {{ ref('fact_menus') }}
    where
        is_dish is true
        and menu_recipe_id is not null
        and fk_dim_dates >= 20190101
        and company_id in ({{ var('active_company_ids') | join(', ') }})

)

, dim_companies as (

    select * from {{ ref('dim_companies') }}

)

, dim_recipes as (

    select * from {{ ref('dim_recipes') }}

)

, dim_ingredient_combinations as (

    select * from {{ ref('dim_ingredient_combinations') }}

)

, dim_portions as (

    select * from {{ ref('dim_portions') }}

)

, dim_taxonomies as (

    select * from {{ ref('dim_taxonomies') }}

)

, recipe_taxonomies as (

    select * from {{ ref('bridge_dim_recipes_dim_taxonomies') }}

)

, recipe_step_sections as (

    select * from {{ ref('pim__recipe_step_sections') }}

)

, recipe_steps as (

    select * from {{ ref('pim__recipe_steps') }}

)

, taxonomy_flags (
    select
        pk_dim_taxonomies
        , cast(
            lower(taxonomy_name_local) like '%inspirasjon%'
            or lower(taxonomy_name_local) like 'inspirerende'
            or lower(taxonomy_name_local) like 'favoritter'
            or lower(taxonomy_name_local) like 'chefs choice'
            or lower(taxonomy_name_local) like 'kockens val'
            or lower(taxonomy_name_local) like '%inspirerande%'
            as int
        ) as has_chefs_favorite_taxonomy
        , cast(
            lower(taxonomy_name_local) like '%family%'
            or lower(taxonomy_name_local) like 'barnevennlig'
            or lower(taxonomy_name_local) like 'familie%'
            or lower(taxonomy_name_local) like 'barnvänlig%'
            or lower(taxonomy_name_local) like 'børnevenlig'
            as int
        ) as has_family_friendly_taxonomy
        , cast(
            lower(taxonomy_name_local) like '%ekspress%'
            or lower(taxonomy_name_local) like 'rask'
            or lower(taxonomy_name_local) like 'laget på 1-2-3'
            or lower(taxonomy_name_local) like 'fort gjort'
            or lower(taxonomy_name_local) like 'snabb%'
            or lower(taxonomy_name_local) like 'enkelt'
            or lower(taxonomy_name_local) like 'hurtig%'
            or lower(taxonomy_name_local) like 'nem på 5'
            as int
        ) as has_quick_and_easy_taxonomy
        , cast(
            lower(taxonomy_name_local) like 'vegetar%'
            or lower(taxonomy_name_local) like 'vegan'
            as int
        ) as has_vegetarian_taxonomy
        , cast(
            lower(taxonomy_name_local) like 'low calorie'
            or lower(taxonomy_name_local) like 'sunn%'
            or lower(taxonomy_name_local) like '%sunt%'
            or lower(taxonomy_name_local) like 'roede'
            or lower(taxonomy_name_local) like 'kalorismart'
            or lower(taxonomy_name_local) like '%viktväktarna%'
            or lower(taxonomy_name_local) like '%sund%'
            or lower(taxonomy_name_local) like '%kalorilet%'
            as int
        ) as has_low_calorie_taxonomy
    from dim_taxonomies
)

, taxonomies_list as (
    select
        dim_recipes.recipe_id
        , concat_ws(', ', collect_list(dim_taxonomies.taxonomy_name_local)) as taxonomy_list
        , size(collect_set(dim_taxonomies.taxonomy_name_local))             as number_of_taxonomies
        , sum(taxonomy_flags.has_chefs_favorite_taxonomy)
        > 0                                                                 as has_chefs_favorite_taxonomy
        , sum(taxonomy_flags.has_quick_and_easy_taxonomy)
        > 0                                                                 as has_quick_and_easy_taxonomy
        , sum(taxonomy_flags.has_vegetarian_taxonomy)
        > 0                                                                 as has_vegetarian_taxonomy
        , sum(taxonomy_flags.has_low_calorie_taxonomy)
        > 0                                                                 as has_low_calorie_taxonomy
        , sum(taxonomy_flags.has_family_friendly_taxonomy)
        > 0                                                                 as has_family_friendly_taxonomy
    from dim_recipes
    left join recipe_taxonomies
        on dim_recipes.pk_dim_recipes = recipe_taxonomies.fk_dim_recipes
    left join dim_taxonomies
        on recipe_taxonomies.fk_dim_taxonomies = dim_taxonomies.pk_dim_taxonomies
    left join taxonomy_flags
        on recipe_taxonomies.fk_dim_taxonomies = taxonomy_flags.pk_dim_taxonomies
    group by 1
)

, ingredient_list as (
    select
        pk_dim_ingredient_combinations
        , ingredient_id_list_array       as ingredient_id_list
        , size(ingredient_id_list_array) as number_of_ingredients
    from dim_ingredient_combinations
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

, distinct_recipes as (
    select distinct
        fact_menus.fk_dim_companies
        , fact_menus.fk_dim_recipes
        , dim_companies.company_id
        , dim_companies.language_id
        , dim_recipes.recipe_id
        , fact_menus.recipe_portion_id
        , dim_recipes.cooking_time_from
        , dim_recipes.cooking_time_to
        , (dim_recipes.cooking_time_from + dim_recipes.cooking_time_to) / 2 as cooking_time_mean
        , dim_recipes.recipe_difficulty_level_id
        , dim_recipes.recipe_main_ingredient_id
        , taxonomies_list.taxonomy_list
        , taxonomies_list.number_of_taxonomies
        , taxonomies_list.has_chefs_favorite_taxonomy
        , taxonomies_list.has_quick_and_easy_taxonomy
        , taxonomies_list.has_vegetarian_taxonomy
        , taxonomies_list.has_low_calorie_taxonomy
        , taxonomies_list.has_family_friendly_taxonomy
        , ingredient_list.ingredient_id_list
        , ingredient_list.number_of_ingredients
        , recipe_steps_list.recipe_step_id_list
        , recipe_steps_list.number_of_recipe_steps
    from fact_menus
    left join dim_companies
        on fact_menus.fk_dim_companies = dim_companies.pk_dim_companies
    left join dim_recipes
        on fact_menus.fk_dim_recipes = dim_recipes.pk_dim_recipes
    left join dim_portions
        on fact_menus.fk_dim_portions = dim_portions.pk_dim_portions
    left join taxonomies_list
        on dim_recipes.recipe_id = taxonomies_list.recipe_id
    left join ingredient_list
        on fact_menus.fk_dim_ingredient_combinations = ingredient_list.pk_dim_ingredient_combinations
    left join recipe_steps_list
        on fact_menus.recipe_portion_id = recipe_steps_list.recipe_portion_id
    where
        dim_portions.portion_name_local = '4'
        and dim_recipes.recipe_id is not null
)

select * from distinct_recipes;
