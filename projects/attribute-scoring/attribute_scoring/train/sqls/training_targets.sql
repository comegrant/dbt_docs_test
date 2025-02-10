with training_targets as (
    select
        recipe_id,
        recipe_portion_id,
        language_id,
        {target_label}
    from mlfeatures.ft_ml_recipes
    where company_id = '{company_id}'
)

select * from training_targets
