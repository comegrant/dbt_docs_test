with fact_menus as (

    select * from {{ ref('fact_menus') }}
    where
        is_dish is true
        and menu_recipe_id is not null
        and fk_dim_dates >= 20190101
        and company_id in ({{ var('active_company_ids') | join(', ') }})

)

, fact_recipe_ingredients as (

    select * from {{ ref('fact_recipe_ingredients') }}

)

, dim_companies as (

    select * from {{ ref('dim_companies') }}

)

, dim_recipes as (

    select * from {{ ref('dim_recipes') }}

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

, taxonomies_list as (
    select
        dim_recipes.recipe_id
        , concat_ws(', ', collect_list(dim_taxonomies.taxonomy_name_local)) as taxonomy_list
        , size(collect_set(dim_taxonomies.taxonomy_name_local))             as number_of_taxonomies
    from dim_recipes
    left join recipe_taxonomies
        on dim_recipes.pk_dim_recipes = recipe_taxonomies.fk_dim_recipes
    left join dim_taxonomies
        on recipe_taxonomies.fk_dim_taxonomies = dim_taxonomies.pk_dim_taxonomies
    group by 1
)

, ingredients_list as (
    select
        recipe_portion_id
        , concat_ws(', ', collect_set(ingredient_id)) as ingredient_id_list
        , size(collect_set(ingredient_id))            as number_of_ingredients
    from fact_recipe_ingredients
    where ingredient_id is not null
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
        , dim_recipes.recipe_difficulty_level_id
        , dim_recipes.recipe_main_ingredient_id
        , taxonomies_list.taxonomy_list
        , taxonomies_list.number_of_taxonomies
        , ingredients_list.ingredient_id_list
        , ingredients_list.number_of_ingredients
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
    left join ingredients_list
        on fact_menus.recipe_portion_id = ingredients_list.recipe_portion_id
    left join recipe_steps_list
        on fact_menus.recipe_portion_id = recipe_steps_list.recipe_portion_id
    where
        dim_portions.portion_name_local = '4'
        and dim_recipes.recipe_id is not null
)

select * from distinct_recipes
