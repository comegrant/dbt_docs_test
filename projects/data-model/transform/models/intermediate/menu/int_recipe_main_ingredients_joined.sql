with

main_ingredients as (

    select * from {{ ref('sil_pim__recipe_main_ingredients') }}

),

main_ingredient_translations as (

    select * from {{ ref('sil_pim__recipe_main_ingredient_translations') }}

),

join_tables as (

select

    main_ingredients.recipe_main_ingredient_id
    , main_ingredients.recipe_main_ingredient_status_code_id
    , main_ingredient_translations.language_id

    {# strings #}
    , main_ingredient_translations.recipe_main_ingredient_name
    , main_ingredient_translations.recipe_main_ingredient_description


from main_ingredients
left join main_ingredient_translations
on main_ingredients.recipe_main_ingredient_id = main_ingredient_translations.recipe_main_ingredient_id
    
)

select * from join_tables