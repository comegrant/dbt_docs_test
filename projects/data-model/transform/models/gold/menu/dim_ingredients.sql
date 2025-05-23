with 

ingredients as (
    select * from {{ ref('pim__ingredients') }}
)

, ingredient_translations as (
    select * from {{ ref('pim__ingredient_translations') }}
)

, category_hierarchy as (
    select * from {{ ref('int_ingredient_category_hierarchies') }}
)

, ingredient_info as (
    select
        ingredients.ingredient_id
        , ingredients.ingredient_internal_reference
        , ingredient_translations.ingredient_name
        , ingredient_translations.language_id
    from ingredients
    
    left join ingredient_translations
        on ingredients.ingredient_id = ingredient_translations.ingredient_id
)

, group_name_extraction as (
    select
        ingredient_id
        , language_id
        , any_value(case when ingredient_category_description = 'Main Group' then ingredient_category_name end) ignore nulls as main_group
        , any_value(case when ingredient_category_description = 'Category Group' then ingredient_category_name end) ignore nulls as category_group
        , any_value(case when ingredient_category_description = 'Product Group' then ingredient_category_name end) ignore nulls as product_group
    from category_hierarchy
    group by 1, 2
)

, flat_hierarchy as (
    select
        ingredient_id
        , language_id
        , any_value(case when hierarchy_level = 0 then ingredient_category_id end) ignore nulls as category_level1
        , any_value(case when hierarchy_level = 1 then ingredient_category_id end) ignore nulls as category_level2
        , any_value(case when hierarchy_level = 2 then ingredient_category_id end) ignore nulls as category_level3
        , any_value(case when hierarchy_level = 3 then ingredient_category_id end) ignore nulls as category_level4
        , any_value(case when hierarchy_level = 4 then ingredient_category_id end) ignore nulls as category_level5
    from category_hierarchy
    group by 1, 2
)


, all_tables_joined as (
    select
        md5(concat_ws(
            '-'
            , ingredient_info.ingredient_id
            , ingredient_info.ingredient_internal_reference
            , ingredient_info.language_id
        )) as pk_dim_ingredients
        , ingredient_info.ingredient_id
        , ingredient_info.ingredient_internal_reference
        , ingredient_info.language_id
        , ingredient_info.ingredient_name
        , group_name_extraction.main_group
        , group_name_extraction.category_group
        , group_name_extraction.product_group
        , flat_hierarchy.category_level1
        , flat_hierarchy.category_level2
        , flat_hierarchy.category_level3
        , flat_hierarchy.category_level4
        , flat_hierarchy.category_level5
    from ingredient_info

    left join group_name_extraction
        on
            ingredient_info.ingredient_id = group_name_extraction.ingredient_id
            and ingredient_info.language_id = group_name_extraction.language_id
    left join flat_hierarchy
        on
            ingredient_info.ingredient_id = flat_hierarchy.ingredient_id
            and ingredient_info.language_id = flat_hierarchy.language_id
    order by ingredient_info.ingredient_id
)

select * from all_tables_joined
