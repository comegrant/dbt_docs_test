with 

recipes as (
    select * from {{ ref('dim_recipes') }}
),

taxonomies as (
    select * from {{ ref('dim_taxonomies') }}
),

recipe_taxonomies as (
    select * from {{ ref('pim__recipe_taxonomies') }}
),

add_fks as (
    select
        md5(cast(concat(recipes.pk_dim_recipes, taxonomies.pk_dim_taxonomies) as string)) as pk_bridge_dim_recipes_dim_taxonomies
        , recipes.pk_dim_recipes as fk_dim_recipes
        , taxonomies.pk_dim_taxonomies as fk_dim_taxonomies

    from recipe_taxonomies
    left join recipes on recipes.recipe_id = recipe_taxonomies.recipe_id
    left join taxonomies on taxonomies.taxonomy_id = recipe_taxonomies.taxonomy_id and taxonomies.language_id = recipes.language_id
    where taxonomies.language_id is not null -- not all taxonomies have a translation for all languages. There are cases where a recipe has several translations
)

select * from add_fks