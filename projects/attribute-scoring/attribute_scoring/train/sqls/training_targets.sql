with recipe_data as (
    select
    *
    from mlgold.attribute_scoring_recipes
    where company_id = '{company_id}'
)

, ingredient_data as (
    select
    *
    from mlgold.dishes_forecasting_recipe_ingredients
)

, training_data as (
    select
    recipe_data.*,
    ingredient_data.*
    from recipe_data
    left join ingredient_data
        on recipe_data.recipe_portion_id = ingredient_data.recipe_portion_id
        and recipe_data.language_id = ingredient_data.language_id
)

select * from training_data
