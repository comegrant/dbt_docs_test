with dim_companies as (
    select
        pk_dim_companies,
        company_id,
        language_id
    from {env}.gold.dim_companies
    where company_id = '{company_id}'
),

recipe_in_menu as (
    select distinct
        fk_dim_recipes,
        company_id
    from {env}.gold.fact_menus
    where (menu_year * 100 + menu_week) >= {start_yyyyww}
),

main_recipe_ids as (
    select distinct
        pk_dim_recipes,
        main_recipe_id,
        recipe_name,
        recipe_main_ingredient_name_english
    from {env}.gold.dim_recipes
    where recipe_main_ingredient_name_english is not null
),

recipe_features as (
    select
        fk_dim_recipes,
        fk_dim_companies,
        recipe_portion_id,
        main_recipe_id,
        cooking_time_mean,
        recipe_difficulty_level_id,
        number_of_recipe_steps,
        number_of_taxonomies,
        number_of_ingredients,
        int(has_chefs_favorite_taxonomy) as has_chefs_favorite_taxonomy,
        int(has_quick_and_easy_taxonomy) as has_quick_and_easy_taxonomy,
        int(has_vegetarian_taxonomy) as has_vegetarian_taxonomy,
        int(has_low_calorie_taxonomy) as has_low_calorie_taxonomy
    from {env}.mlfeatures.ft_ml_recipes
),

dim_ingredients as (
    select distinct
        ingredient_id,
        allergy_id,
        ingredient_name,
        allergy_name
    from {env}.gold.dim_ingredients
    where allergy_id is not null
),

recipe_ingredients as (
    select
        recipe_portion_id,
        explode(split(ingredient_id_list, ',')) as ingredient_id
    from {env}.mlfeatures.ft_recipe_ingredients
),

allergens as (
    select
        ingredient_id,
        allergy_id
    from {env}.gold.dim_ingredients
    where allergy_id is not null
),

recipe_allergies as (
    select
        recipe_portion_id,
        recipe_ingredients.ingredient_id,
        allergy_id
    from recipe_ingredients
    inner join
        allergens
    on allergens.ingredient_id = recipe_ingredients.ingredient_id
),

recipe_allergy_agged as (
    select
        recipe_portion_id,
        array_agg(distinct(allergy_id)) as allergen_id_list
    from recipe_allergies
    group by recipe_portion_id
)


select distinct
    main_recipe_ids.main_recipe_id,
    recipe_name,
    recipe_main_ingredient_name_english,
    cooking_time_mean,
    recipe_difficulty_level_id,
    number_of_recipe_steps,
    number_of_taxonomies,
    number_of_ingredients,
    has_chefs_favorite_taxonomy,
    has_quick_and_easy_taxonomy,
    has_vegetarian_taxonomy,
    has_low_calorie_taxonomy,
    allergen_id_list
from
    dim_companies
left join
    recipe_in_menu
on dim_companies.company_id = recipe_in_menu.company_id
inner join
    main_recipe_ids
    on main_recipe_ids.pk_dim_recipes = recipe_in_menu.fk_dim_recipes
left join
    recipe_features
    on dim_companies.pk_dim_companies = recipe_features.fk_dim_companies
    and main_recipe_ids.pk_dim_recipes = recipe_features.fk_dim_recipes
left join recipe_allergy_agged
on recipe_allergy_agged.recipe_portion_id = recipe_features.recipe_portion_id
