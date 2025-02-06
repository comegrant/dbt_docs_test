with

chef_ingredient_sections as (
    select
        recipe_portion_id
        , chef_ingredient_section_id
    from {{ ref('pim__chef_ingredient_sections') }}
)

, chef_ingredients as (
    select
        chef_ingredient_section_id
        , generic_ingredient_id
        , order_ingredient_id
    from {{ ref("pim__chef_ingredients") }}
)

, generic_ingredient_names as (
    select
        generic_ingredient_id
        , language_id
        , generic_ingredient_name
    from {{ ref("pim__generic_ingredient_translations") }}
)

{# joining all 3 CTEs above #}
, recipes_generic_ingredient_names_joined as (
    select
        chef_ingredient_sections.recipe_portion_id
        , chef_ingredients.generic_ingredient_id
        , generic_ingredient_names.language_id
        , generic_ingredient_names.generic_ingredient_name
        , chef_ingredients.order_ingredient_id
    from chef_ingredient_sections
    left join chef_ingredients
        on chef_ingredients.chef_ingredient_section_id = chef_ingredient_sections.chef_ingredient_section_id
    left join generic_ingredient_names
        on generic_ingredient_names.generic_ingredient_id = chef_ingredients.generic_ingredient_id
)

, order_ingredients as (
    select
        order_ingredient_id
        , ingredient_internal_reference
        , is_main_carbohydrate
        , is_main_protein
    from {{ ref("pim__order_ingredients") }}
)

, ingredient_categories as (
    select
        ingredient_id
        , ingredient_category_id
        , ingredient_internal_reference
    from {{ ref("pim__ingredients") }}
)

{# to map each order_ingredient_id with category_id #}
, order_ingredients_categories_linking as (
    select
        order_ingredients.order_ingredient_id
        , order_ingredients.is_main_carbohydrate
        , order_ingredients.is_main_protein
        , ingredient_categories.ingredient_id
        , ingredient_categories.ingredient_category_id
    from order_ingredients
    left join ingredient_categories
        on
            order_ingredients.ingredient_internal_reference
            = ingredient_categories.ingredient_internal_reference
)

{# Joining the two joined CTEs together through order_ingredient_id #}
, recipes_generic_ingredient_names_categories_joined as (
    select
        recipes_generic_ingredient_names_joined.*
        , order_ingredients_categories_linking.ingredient_id
        , order_ingredients_categories_linking.ingredient_category_id
        , order_ingredients_categories_linking.is_main_carbohydrate
        , order_ingredients_categories_linking.is_main_protein
    from recipes_generic_ingredient_names_joined
    left join order_ingredients_categories_linking
        on
            recipes_generic_ingredient_names_joined.order_ingredient_id
            = order_ingredients_categories_linking.order_ingredient_id
    where
        recipes_generic_ingredient_names_joined.generic_ingredient_id is not null
        and recipes_generic_ingredient_names_joined.generic_ingredient_name is not null
        and order_ingredients_categories_linking.ingredient_id is not null
)

select * from recipes_generic_ingredient_names_categories_joined
