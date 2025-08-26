with predictions as (
    select distinct
        company_id,
        recipe_id
    from gold.fact_menus
    where (menu_year*100 + menu_week) between {start_yyyyww} and {end_yyyyww}
    and company_id = '{company_id}'
    and recipe_id is not null
    and portion_id = 2 -- 4 portions
),

recipe_features as (
    select
        *
    from mlgold.attribute_scoring_recipes
    where company_id = '{company_id}'
)

, ingredient_features as (
    select
        *
    from mlgold.dishes_forecasting_recipe_ingredients
)

, prediction_data as (
    select
        predictions.company_id,
        predictions.recipe_id,
        recipe_features.recipe_portion_id,
        recipe_features.language_id,
        {recipe_columns_prefixed},
        {ingredient_columns_prefixed}
    from predictions
    left join recipe_features
        on predictions.recipe_id = recipe_features.recipe_id
    left join ingredient_features
        on recipe_features.recipe_portion_id = ingredient_features.recipe_portion_id
        and recipe_features.language_id = ingredient_features.language_id
)

select * from prediction_data
