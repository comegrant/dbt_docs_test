with predictions as (
    select distinct
        recipe_id
    from {input_schema}.{input_table}
    where (menu_year*100 + menu_week) between {start_yyyyww} and {end_yyyyww}
    and company_id = '{company_id}'
),

features as (
    select
        recipe_id,
        recipe_portion_id,
        language_id
    from mlfeatures.ft_ml_recipes
    where company_id = '{company_id}'
)

select
    predictions.recipe_id
from predictions
left join features
    on predictions.recipe_id = features.recipe_id
where features.recipe_id is null -- queries recipe_ids in fact_menus that do not yet exist in mlfeatures.ft_ml_recipes
