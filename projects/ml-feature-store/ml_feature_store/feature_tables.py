from pydantic import BaseModel


class FeatureTable(BaseModel):
    feature_table_name: str
    primary_keys: list[str]
    ml_feature_schema: str = "mlfeatures"
    dbt_model_name: str
    dbt_model_schema: str = "mlgold"


ft_recipe_ingredients_configs = FeatureTable(
    feature_table_name="ft_recipe_ingredients",
    primary_keys=["recipe_portion_id", "language_id"],
    dbt_model_name="ingredients_aggregated_by_recipe_portion_id",
)
