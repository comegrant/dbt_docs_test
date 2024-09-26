from pydantic import BaseModel


class FeatureTable(BaseModel):
    feature_table_name: str
    primary_keys: list[str]
    ml_feature_schema: str = 'mlfeatures'
    dbt_model_name: str
    dbt_model_schema: str = 'mlgold'


ft_weekly_dishes_variations_configs = FeatureTable(
    feature_table_name='ft_weekly_dishes_variations',
    primary_keys=['menu_year', 'menu_week', 'company_id', 'product_variation_id'],
    dbt_model_name='weekly_dishes_variations'
)

ft_ml_recipes_configs = FeatureTable(
    feature_table_name='ft_ml_recipes',
    primary_keys=['recipe_id'],
    dbt_model_name='ml_recipes'
)

ft_recipe_ingredients_configs = FeatureTable(
    feature_table_name='ft_recipe_ingredients',
    primary_keys=['recipe_portion_id', 'language_id'],
    dbt_model_name='ingredients_aggregated_by_recipe_portion_id'
)
