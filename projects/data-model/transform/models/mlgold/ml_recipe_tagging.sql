with

dim_recipes as (
    select * from {{ ref('dim_recipes') }}
)

, dim_taxonomies as (
    select * from {{ ref('dim_taxonomies') }}
)

, dim_preference_combinations as (
    select * from {{ ref('dim_preference_combinations') }}
)

, bridge_recipes_taxonomies as (
    select * from {{ ref('bridge_dim_recipes_dim_taxonomies') }}
)

, recipe_preferences_unioned as (
    select * from {{ ref('int_recipe_preferences_unioned') }}
)

, portions as (
    select * from {{ ref('pim__recipe_portions') }}
)

, ingredient_sections as (
    select * from {{ ref('pim__chef_ingredient_sections') }}
)

, chef_ingredients (
    select * from {{ ref('pim__chef_ingredients') }}
)

, ingredient_translations (
    select * from {{ ref('pim__generic_ingredient_translations') }}
)

, recipe_nutritional_facts (
    select * from {{ ref('ml_recipe_nutritional_facts') }}
)


, recipes as (
    select
        pk_dim_recipes
        , main_recipe_id
        , recipe_id
        , recipe_name
        , recipe_main_ingredient_id
        , recipe_main_ingredient_name_local
        , recipe_difficulty_level_id
        , cooking_time_from
        , cooking_time_to
        , language_id
        , is_in_recipe_universe
    from dim_recipes
    where is_in_recipe_universe = true
)

, taxonomies as (
    select
        pk_dim_taxonomies
        , taxonomy_id
        , language_id
        , taxonomy_name_local
    from dim_taxonomies
    where taxonomy_type_name not like 'seo_%'
)

, taxonomy_list as (
    select
        recipes.recipe_id
        , taxonomies.language_id
        , concat_ws(', ', collect_list(taxonomies.taxonomy_name_local)) as taxonomy_name_list
        , concat_ws(', ', collect_list(taxonomies.taxonomy_id))         as taxonomy_id_list
        , size(collect_set(taxonomies.taxonomy_name_local))             as number_of_taxonomies
    from recipes
    left join bridge_recipes_taxonomies
        on recipes.pk_dim_recipes = bridge_recipes_taxonomies.fk_dim_recipes
    left join taxonomies
        on bridge_recipes_taxonomies.fk_dim_taxonomies = taxonomies.pk_dim_taxonomies
    group by 1, 2
)

, ingredients as (
    select
        portions.recipe_id
        , chef_ingredients.generic_ingredient_id
        , ingredient_translations.generic_ingredient_name
        , ingredient_translations.language_id
    from portions
    left join ingredient_sections
        on portions.recipe_portion_id = ingredient_sections.recipe_portion_id
    left join chef_ingredients
        on
            ingredient_sections.chef_ingredient_section_id
            = chef_ingredients.chef_ingredient_section_id
    left join ingredient_translations
        on chef_ingredients.generic_ingredient_id = ingredient_translations.generic_ingredient_id
    where chef_ingredients.generic_ingredient_id is not null
)

, ingredient_list as (
    select
        recipes.recipe_id
        , concat_ws(', ', collect_set(ingredients.generic_ingredient_name)) as generic_ingredient_name_list
        , concat_ws(', ', collect_set(ingredients.generic_ingredient_id))   as generic_ingredient_id_list
        , size(collect_set(ingredients.generic_ingredient_id))              as number_of_ingredients
    from recipes
    left join ingredients
        on
            recipes.recipe_id = ingredients.recipe_id
            and recipes.language_id = ingredients.language_id
    group by 1
)

, recipe_allergens as (
    select
        recipes.recipe_id
        , dim_preference_combinations.preference_name_combinations
        , dim_preference_combinations.allergen_name_combinations
        , dim_preference_combinations.taste_name_combinations_excluding_allergens
    from recipes
    left join recipe_preferences_unioned
        on recipes.recipe_id = recipe_preferences_unioned.recipe_id
    left join dim_preference_combinations
        on
            recipe_preferences_unioned.preference_combination_id
            = dim_preference_combinations.pk_dim_preference_combinations
)

, nutrition as (
    select *
    from (
        select
            recipe_id
            , portion_size
            , protein_gram_per_portion
            , carbs_gram_per_portion
            , fat_gram_per_portion
            , sat_fat_gram_per_portion
            , sugar_gram_per_portion
            , sugar_added_gram_per_portion
            , fiber_gram_per_portion
            , salt_gram_per_portion
            , fg_fresh_gram_per_portion
            , fg_proc_gram_per_portion
            , total_kcal_per_portion
            , row_number() over (
                partition by recipe_id
                order by
                    case when portion_size = 4 then 0 else 1 end-- prioritize 4
                    , portion_size  -- fallback: lowest portion size
            ) as rn
        from recipe_nutritional_facts
        where portion_size in (1, 2, 3, 4, 5, 6)
    ) as ranked
    where rn = 1
)

, final as (
    select distinct
        recipes.recipe_id
        , recipes.recipe_name
        , recipes.recipe_main_ingredient_name_local
        , recipes.language_id
        , recipes.cooking_time_to
        , taxonomy_list.taxonomy_name_list
        , ingredient_list.generic_ingredient_name_list
        , recipe_allergens.preference_name_combinations
        , coalesce(
            nutrition.total_kcal_per_portion
            < case
                when recipes.language_id = 1 then 750 --norway
                when recipes.language_id = 5 then 550 -- sweden
                when recipes.language_id = 6 then 600 --denmark
                else 0
            end
            and (nutrition.fg_fresh_gram_per_portion + nutrition.fg_proc_gram_per_portion) > 150
            , false
        ) as is_low_calorie
        , coalesce(
            nutrition.fiber_gram_per_portion > 10
            , false
        ) as is_high_fiber
        , coalesce(
            nutrition.fat_gram_per_portion < (nutrition.total_kcal_per_portion * 0.3 / 9)
            , false
        ) as is_low_fat
        , coalesce(
            nutrition.sugar_gram_per_portion < (nutrition.total_kcal_per_portion * 0.07 / 4), false
        ) as is_low_sugar
        , nutrition.total_kcal_per_portion
        , nutrition.protein_gram_per_portion
        , nutrition.carbs_gram_per_portion
        , nutrition.fat_gram_per_portion
    from recipes
    left join taxonomy_list
        on recipes.recipe_id = taxonomy_list.recipe_id
    left join ingredient_list
        on recipes.recipe_id = ingredient_list.recipe_id
    left join recipe_allergens
        on recipes.recipe_id = recipe_allergens.recipe_id
    left join nutrition
        on recipes.recipe_id = nutrition.recipe_id
    where recipes.language_id = taxonomy_list.language_id
)

select * from final
