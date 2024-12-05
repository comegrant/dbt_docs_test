with recipes_to_train as (
    select distinct
        menu_year,
        menu_week,
        company_id,
        recipe_id
    from
        mlgold.weekly_dishes_variations
),

train_target as (
    select
        recipe_id,
        recipe_difficulty_level_id
    from mlfeatures.ft_ml_recipes
),

target_set as (
    select
        recipes_to_train.recipe_id,
        recipe_difficulty_level_id
    from
        recipes_to_train
    left join
        train_target
    on
        recipes_to_train.recipe_id = train_target.recipe_id
    where (menu_year * 100 + menu_week) >= {train_start_yyyyww}
        and  (menu_year * 100 + menu_week) <= {train_end_yyyyww}
        and company_id = '{company_id}'
        and recipe_difficulty_level_id is not null
)

select * from target_set
