with

ingredients as (

    select * from {{ ref('pim__ingredients') }}

)

, ingredient_categories as (

    select * from {{ ref('pim__ingredient_categories') }}

)

, ingredient_category_translations as (
    select * from {{ ref('pim__ingredient_category_translations') }}
)

, base_level as (
    select
        ingredient_id
        , ingredient_category_id
    from ingredients
    where is_active_ingredient = True -- active
)

, parent_level as (
    select
        ingredient_category_id
        , parent_category_id
    from ingredient_categories
    where ingredient_category_status_code_id = 1 -- active
)

, category_hierarchy as (
    select
        level_1.ingredient_id
        , level_1.ingredient_category_id
        , level_2.parent_category_id
        , 0                            as hierarchy_level
    from base_level as level_1
    left join parent_level as level_2
        on level_1.ingredient_category_id = level_2.ingredient_category_id

    union all

    select
        level_1.ingredient_id
        , level_2.parent_category_id as ingredient_category_id
        , level_3.parent_category_id
        , 1               as hierarchy_level
    from base_level as level_1
    left join parent_level as level_2
        on level_1.ingredient_category_id = level_2.ingredient_category_id
    left join parent_level as level_3
        on level_2.parent_category_id = level_3.ingredient_category_id

    union all

    select
        level_1.ingredient_id
        , level_3.parent_category_id as ingredient_category_id
        , level_4.parent_category_id
        , 2               as hierarchy_level
    from base_level as level_1
    left join parent_level as level_2
        on level_1.ingredient_category_id = level_2.ingredient_category_id
    left join parent_level as level_3
        on level_2.parent_category_id = level_3.ingredient_category_id
    left join parent_level as level_4
        on level_3.parent_category_id = level_4.ingredient_category_id

    union all

    select
        level_1.ingredient_id
        , level_4.parent_category_id as ingredient_category_id
        , level_5.parent_category_id
        , 3               as hierarchy_level
    from base_level as level_1
    left join parent_level as level_2
        on level_1.ingredient_category_id = level_2.ingredient_category_id
    left join parent_level as level_3
        on level_2.parent_category_id = level_3.ingredient_category_id
    left join parent_level as level_4
        on level_3.parent_category_id = level_4.ingredient_category_id
    left join parent_level as level_5
        on level_4.parent_category_id = level_5.ingredient_category_id

    union all

    select
        level_1.ingredient_id
        , level_5.parent_category_id as ingredient_category_id
        , level_6.parent_category_id
        , 4               as hierarchy_level
    from base_level as level_1
    left join parent_level as level_2
        on level_1.ingredient_category_id = level_2.ingredient_category_id
    left join parent_level as level_3
        on level_2.parent_category_id = level_3.ingredient_category_id
    left join parent_level as level_4
        on level_3.parent_category_id = level_4.ingredient_category_id
    left join parent_level as level_5
        on level_4.parent_category_id = level_5.ingredient_category_id
    left join parent_level as level_6
        on level_5.parent_category_id = level_6.ingredient_category_id

),

category_hierarchy_with_translations as (
    select
        category_hierarchy.ingredient_id
        , category_hierarchy.ingredient_category_id
        , category_hierarchy.hierarchy_level
        , ingredient_category_translations.ingredient_category_name
        , ingredient_category_translations.ingredient_category_description
        , ingredient_category_translations.language_id
    from category_hierarchy
    left join ingredient_category_translations
        on category_hierarchy.ingredient_category_id = ingredient_category_translations.ingredient_category_id
    where category_hierarchy.ingredient_category_id is not null
    order by ingredient_id, hierarchy_level
)

select * from category_hierarchy_with_translations
