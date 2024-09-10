from pydantic import BaseModel


class FeatureTable(BaseModel):
    feature_table_name: str
    primary_keys: list[str]
    ml_feature_schema: str = 'mlfeatures'
    dbt_model_name: str
    dbt_model_schema: str = 'mlgold'


ft_weekly_dishes_variations_configs = FeatureTable(
    feature_table_name='ft_weekly_dishes_variations',
    primary_keys=['year', 'week', 'company_id', 'product_variation_id'],
    dbt_model_name='weekly_dishes_variations'
)
