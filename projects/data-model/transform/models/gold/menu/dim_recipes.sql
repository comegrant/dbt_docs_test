with 

recipes as (

    select * from {{ ref('pim__recipes') }}

),

recipe_metadata as (

    select * from {{ ref('int_recipe_metadata_joined') }}

),

recipe_main_ingredients as (

    select * from {{ ref('int_recipe_main_ingredients_joined') }}

),

join_tables as (

select

    md5(cast(concat(recipes.recipe_id, recipe_metadata.language_id) as string)) as pk_dim_recipes
    , recipes.recipe_id
    , recipes.recipe_metadata_id
    , coalesce(recipes.main_recipe_id, recipes.recipe_id) as main_recipe_id
    , recipes.recipe_status_code_id
    , recipe_metadata.recipe_main_ingredient_id
    , recipe_metadata.recipe_difficulty_level_id
    , recipe_metadata.language_id

    , recipe_metadata.cooking_time_from
    , recipe_metadata.cooking_time_to

    , recipe_metadata.recipe_name
    --, recipe_metadata.recipe_photo_caption
    --, recipe_metadata.roede_calculation_text
    --, recipe_metadata.recipe_extra_photo_caption
    --, recipe_metadata.recipe_general_text
    --, recipe_metadata.recipe_description
    , recipe_metadata.recipe_difficulty_name
    --, recipe_metadata.recipe_difficulty_description

    , recipe_main_ingredients.recipe_main_ingredient_name
    --, recipe_main_ingredients.recipe_main_ingredient_description

    , coalesce(main_recipe_metadata.recipe_name, recipe_metadata.recipe_name) as main_recipe_name
    --, main_recipe_metadata.recipe_description as main_recipe_description
    , case when recipes.main_recipe_id is null then true else false end as is_main_recipe


from recipes
left join recipe_metadata
on recipes.recipe_metadata_id = recipe_metadata.recipe_metadata_id

left join recipe_main_ingredients
on recipe_metadata.recipe_main_ingredient_id = recipe_main_ingredients.recipe_main_ingredient_id
and recipe_metadata.language_id = recipe_main_ingredients.language_id

left join recipes as main_recipes
on recipes.main_recipe_id = main_recipes.recipe_id

left join recipe_metadata as main_recipe_metadata
on main_recipes.recipe_metadata_id = main_recipe_metadata.recipe_metadata_id
and main_recipe_metadata.language_id = recipe_metadata.language_id

)

, add_unknown_row (

    select 
        * 
    from join_tables

    union all

    select 
        '0',
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "Not relevant",
        "Not relevant",
        "Not relevant",
        "Not relevant",
        false
)

select * from add_unknown_row