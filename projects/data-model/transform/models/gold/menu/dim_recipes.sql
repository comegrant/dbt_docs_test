with

-- SILVER
recipes as (

    select * from {{ ref('pim__recipes') }}

)

, recipe_companies as (

    select * from {{ ref('pim__recipe_companies') }}

)

, recipe_translations as (

    select * from {{ ref('pim__recipe_translations') }}

)

-- ASSUMPTION: Only local languages for the brands we deliver to is found in this table
, local_languages as (

    select distinct language_id from {{ ref('cms__countries') }}

)

-- INTERMEDIATE
, recipe_metadata as (

    select * from {{ ref('int_recipe_metadata_joined') }}

)

, recipe_main_ingredients as (

    select * from {{ ref('int_recipe_main_ingredients_joined') }}

)

, main_recipes_in_menus as (

    select * from {{ ref('int_weekly_menu_variations_main_recipes_distinct') }}

)

-- GOLD
, companies as (

    select * from {{ ref('dim_companies') }}

)

-- TRANSFORMATION
, recipe_companies_find_local_language_id as (

    select distinct
        recipe_id
        , companies.language_id

    from recipe_companies

    left join companies
        on recipe_companies.company_id = companies.company_id

)

, recipe_tables_joined as (

    select

        md5(cast(concat(recipes.recipe_id, recipe_metadata.language_id) as string)) as pk_dim_recipes
        , recipes.recipe_id
        , recipes.recipe_metadata_id
        , coalesce(recipes.main_recipe_id, recipes.recipe_id)                       as main_recipe_id
        , recipes.main_recipe_variation_id
        , recipes.main_recipe_variation_suffix
        , recipes.recipe_status_code_id

        , recipe_metadata.recipe_main_ingredient_id
        , recipe_metadata.recipe_difficulty_level_id
        , recipe_metadata.language_id as language_id
        , recipe_metadata.cooking_time_from
        , recipe_metadata.cooking_time_to

        , recipe_metadata.cooking_time
        , recipe_metadata.cooking_time_from*1000+recipe_metadata.cooking_time_to as cooking_time_sorting

        , recipe_metadata.has_recipe_photo

        , recipe_metadata.recipe_name
        , recipe_metadata.recipe_photo
        , recipe_metadata.recipe_photo_caption
        , recipe_metadata.recipe_general_text
        , recipe_metadata.recipe_description
        , recipe_metadata.recipe_difficulty_name

        , recipe_main_ingredients_local.recipe_main_ingredient_name                 as recipe_main_ingredient_name_local
        , recipe_main_ingredients_english.recipe_main_ingredient_name               as recipe_main_ingredient_name_english

        , coalesce(main_recipe_metadata.recipe_name, recipe_metadata.recipe_name)   as main_recipe_name

        , coalesce(recipes.main_recipe_id is null, false)                           as is_main_recipe
        , recipes.is_in_recipe_universe
        , recipes.recipe_shelf_life_days

        , recipe_translations.recipe_comment
        , recipe_translations.recipe_chef_tip

    from recipes

    left join recipe_metadata
        on recipes.recipe_metadata_id = recipe_metadata.recipe_metadata_id

    left join recipe_main_ingredients as recipe_main_ingredients_local
        on
            recipe_metadata.recipe_main_ingredient_id
            = recipe_main_ingredients_local.recipe_main_ingredient_id
            and recipe_metadata.language_id = recipe_main_ingredients_local.language_id

    left join recipe_main_ingredients as recipe_main_ingredients_english
        on
            recipe_metadata.recipe_main_ingredient_id
            = recipe_main_ingredients_english.recipe_main_ingredient_id
            and recipe_main_ingredients_english.language_id = 4 --English

    left join recipes as main_recipes
        on recipes.main_recipe_id = main_recipes.recipe_id

    left join recipe_metadata as main_recipe_metadata
        on
            main_recipes.recipe_metadata_id = main_recipe_metadata.recipe_metadata_id
            and recipe_metadata.language_id = main_recipe_metadata.language_id

    left join recipe_translations as recipe_translations
        on
            recipes.recipe_id = recipe_translations.recipe_id
            and recipe_metadata.language_id = recipe_translations.language_id

    left join recipe_companies_find_local_language_id
        on recipes.recipe_id = recipe_companies_find_local_language_id.recipe_id

    left join local_languages
        on recipe_metadata.language_id = local_languages.language_id

    -- Filter to keep only language_ids which are used as brand language
    -- and to keep only language_ids for companies connected to recipes in recipe_companies
    where local_languages.language_id is not null
        and (
            recipe_companies_find_local_language_id.recipe_id is null
            or recipe_companies_find_local_language_id.language_id = local_languages.language_id
        )

)

, recipes_tables_add_menu_week_information as (

    select
        recipe_tables_joined.*
        , main_recipes_in_menus.menu_week_count_main_recipe
        , main_recipes_in_menus.previous_menu_week_main_recipe
        , main_recipes_in_menus.weeks_since_first_menu_week_main_recipe
    from recipe_tables_joined
    left join main_recipes_in_menus
        on recipe_tables_joined.main_recipe_id = main_recipes_in_menus.main_recipe_id
    -- filter out recipes that are not in recipe universe or menu week
    where
        main_recipes_in_menus.main_recipe_id is not null
        or recipe_tables_joined.is_in_recipe_universe is true

)

, add_unknown_row as (

    select *
    from recipes_tables_add_menu_week_information

    union all

    select
        '0'              as pk_dim_recipes
        , 0              as recipe_id
        , 0              as recipe_metadata_id
        , 0              as main_recipe_id
        , 0              as main_recipe_variation_id
        , 0              as main_recipe_variation_suffix
        , 0              as recipe_status_code_id
        , 0              as recipe_main_ingredient_id
        , 0              as recipe_difficulty_level_id
        , 0              as language_id
        , 0              as cooking_time_from
        , 0              as cooking_time_to
        , 'Not relevant' as cooking_time
        , 0              as cooking_time_sorting
        , false          as has_recipe_photo
        , 'Not relevant' as recipe_name
        , 'Not relevant' as recipe_photo
        , 'Not relevant' as recipe_photo_caption
        , 'Not relevant' as recipe_general_text
        , 'Not relevant' as recipe_description
        , 'Not relevant' as recipe_difficulty_name
        , 'Not relevant' as recipe_main_ingredient_name_local
        , 'Not relevant' as recipe_main_ingredient_name_english
        , 'Not relevant' as main_recipe_name
        , false          as is_main_recipe
        , false          as is_in_recipe_universe
        , 0              as recipe_shelf_life_days
        , 'Not relevant' as recipe_comment
        , 'Not relevant' as recipe_chef_tip
        , 0 as menu_week_count_main_recipe
        , 0 as previous_menu_week_main_recipe
        , 0 as weeks_since_first_menu_week_main_recipe
)

select * from add_unknown_row
