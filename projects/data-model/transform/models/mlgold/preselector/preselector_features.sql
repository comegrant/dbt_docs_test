with

fact_menus as (
    select * from {{ ref('fact_menus') }}
)

, dim_recipes as (
    select * from {{ ref('dim_recipes') }}
)

, dim_taxonomies as (
    select * from {{ ref('dim_taxonomies') }}
)

, bridge_recipe_taxonomies as (
    select * from {{ ref('bridge_dim_recipes_dim_taxonomies') }}
)

, ratings as (
    select * from {{ ref('pim__recipe_ratings') }}
)

, recipes as (
    select distinct
        fact_menus.company_id
        , fact_menus.menu_year
        , fact_menus.menu_week
        , dim_recipes.main_recipe_id
        , dim_recipes.recipe_id
        , dim_recipes.recipe_name
        , dim_recipes.recipe_photo
        , dim_recipes.recipe_main_ingredient_id
        , dim_recipes.cooking_time_from
        , dim_recipes.cooking_time_to
    from fact_menus
    left join dim_recipes
        on fact_menus.recipe_id = dim_recipes.recipe_id
    where
        fact_menus.company_id in ({{ var('active_company_ids') | join(', ') }})
        and fact_menus.recipe_id is not null
        and fact_menus.is_dish = true
)

, taxonomy_list as (
    select
        dim_recipes.recipe_id
        , array_sort(
            array_distinct(
                filter(collect_list(dim_taxonomies.taxonomy_id), x -> x is not null)
            )
        ) as taxonomy_id_list
        , array_sort(
            array_distinct(
                filter(collect_list(dim_taxonomies.taxonomy_type_name), x -> x is not null)
            )
        ) as taxonomy_type_name_list
    from dim_recipes
    left join bridge_recipe_taxonomies
        on dim_recipes.pk_dim_recipes = bridge_recipe_taxonomies.fk_dim_recipes
    left join dim_taxonomies
        on bridge_recipe_taxonomies.fk_dim_taxonomies = dim_taxonomies.pk_dim_taxonomies
    group by 1
)

, recipe_ratings as (
    select
        recipe_id
        , sum(recipe_rating) as sum_rating
        , count(*)           as number_of_ratings
    from ratings
    where is_not_cooked_dish = false
    group by 1
)

, recipe_joined as (
    select
        recipes.*
        , taxonomy_list.taxonomy_id_list
        , taxonomy_list.taxonomy_type_name_list
        , recipe_ratings.sum_rating
        , recipe_ratings.number_of_ratings
    from recipes
    left join taxonomy_list
        on recipes.recipe_id = taxonomy_list.recipe_id
    left join recipe_ratings
        on recipes.recipe_id = recipe_ratings.recipe_id
)

, recipe_aggregated as (
    select
        *
        , count(*) over (
            partition by company_id, main_recipe_id
            order by menu_year, menu_week
            rows between unbounded preceding and current row
        ) as cumulated_times_on_menu
        , sum(sum_rating) over (
            partition by company_id, main_recipe_id
            order by menu_year, menu_week
            rows between unbounded preceding and current row
        ) as cumulated_sum_rating
        , sum(number_of_ratings) over (
            partition by company_id, main_recipe_id
            order by menu_year, menu_week
            rows between unbounded preceding and current row
        ) as cumulated_number_of_ratings
    from recipe_joined
)

, recipe_features as (
    select
        company_id
        , menu_year
        , menu_week
        , main_recipe_id
        , recipe_id
        , recipe_name
        , recipe_photo
        , recipe_main_ingredient_id
        , cooking_time_from
        , cooking_time_to
        , taxonomy_id_list
        , taxonomy_type_name_list
        , cumulated_times_on_menu
        , cumulated_number_of_ratings
        , cumulated_sum_rating / cumulated_number_of_ratings as cumulated_average_rating
    from recipe_aggregated
)

select * from recipe_features
