with 

recipes as (

    select * from {{ ref('pim__recipes') }}

)

, recipe_portions as (

    select * from {{ ref('pim__recipe_portions') }}

)

, chef_ingredient_sections as (

    select * from {{ ref('pim__chef_ingredient_sections') }}

)

, chef_ingredients as (

    select * from {{ ref('pim__chef_ingredients') }}

)

, recipe_ingredients as (

    select * from {{ ref('pim__recipe_ingredients') }}

)

, ingredients as (

    select * from {{ ref('pim__ingredients') }}

)

, ingredient_translations as (

    select * from {{ ref('pim__ingredient_translations') }}

)

, generic_ingredient_translations as (

    select * from {{ ref('pim__generic_ingredient_translations') }}

)

, all_tables_joined as (
    select
        recipes.recipe_id
        , recipe_portions.portion_id
        , chef_ingredient_sections.recipe_portion_id
        , chef_ingredients.chef_ingredient_section_id
        , generic_ingredient_translations.generic_ingredient_id
        , generic_ingredient_translations.generic_ingredient_name
        , ingredients.ingredient_id
        , recipe_ingredients.ingredient_internal_reference
        , ingredient_translations.ingredient_name
        , ingredient_translations.language_id
        , recipe_ingredients.is_main_carbohydrate
        , recipe_ingredients.is_main_protein
        , recipe_ingredients.nutrition_units 
        , recipe_ingredients.recipe_ingredient_quantity
        , ingredients.ingredient_net_weight
        , ingredients.ingredient_co2_emissions_per_kg
        , ingredients.ingredient_co2_emissions_per_unit
        , ingredients.has_co2_data

    from recipes

    left join recipe_portions
        on recipes.recipe_id = recipe_portions.recipe_id
    left join chef_ingredient_sections
        on recipe_portions.recipe_portion_id = chef_ingredient_sections.recipe_portion_id
    left join chef_ingredients
        on chef_ingredient_sections.chef_ingredient_section_id = chef_ingredients.chef_ingredient_section_id
    left join generic_ingredient_translations
        on chef_ingredients.generic_ingredient_id = generic_ingredient_translations.generic_ingredient_id
    left join recipe_ingredients
        on chef_ingredients.recipe_ingredient_id = recipe_ingredients.recipe_ingredient_id
    left join ingredients
        on recipe_ingredients.ingredient_internal_reference = ingredients.ingredient_internal_reference
    left join ingredient_translations
        on ingredients.ingredient_id = ingredient_translations.ingredient_id

    where generic_ingredient_translations.language_id = ingredient_translations.language_id
)

select * from all_tables_joined
