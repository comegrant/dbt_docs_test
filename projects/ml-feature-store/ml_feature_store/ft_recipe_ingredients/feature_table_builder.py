import re
from typing import Literal, Optional

import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel
from pyspark.sql import SparkSession

from ml_feature_store.common.data import add_updated_at, get_data_from_catalog, run_custom_sql, save_df_as_feature_table
from ml_feature_store.feature_tables import ft_recipe_ingredients_configs
from ml_feature_store.ft_recipe_ingredients.category_features_generator import add_category_features
from ml_feature_store.ft_recipe_ingredients.configs.categories import categories
from ml_feature_store.ft_recipe_ingredients.paths import SQL_DIR
from ml_feature_store.ft_recipe_ingredients.text_feature_generator import get_generic_name_lemmatized_nouns


class Args(BaseModel):
    env: Literal["dev", "test", "prod"]
    is_drop_existing: bool = False


def build_feature_table(args: Args, spark: SparkSession) -> None:
    table_config = ft_recipe_ingredients_configs
    df_recipe_ingredients_aggregated = get_data_from_catalog(
        spark=spark,
        env=args.env,
        table_name=table_config.dbt_model_name,
        schema=table_config.dbt_model_schema,
        is_convert_to_pandas=True
    )
    df_distinct_ingredients = get_ingredient_data(
        spark=spark,
        env=args.env,
        schema="mlgold"
    )
    df_ft_recipe_ingredients = get_generic_name_lemmatized_nouns(
        df_recipe_ingredients=df_recipe_ingredients_aggregated,  # type: ignore
        df_distinct_ingredients=df_distinct_ingredients,
        distinct_ingredient_text_col_name="generic_ingredient_name",
        distinct_ingredient_id_column_name="generic_ingredient_id",
        nouns_only_column_name="generic_ingredient_name_nouns_only",
        lemmatized_column_name="generic_ingredient_name_lemmatized_nouns_only"
    )
    df_ft_recipe_ingredients = add_category_features(
        df_recipe_ingredients=df_ft_recipe_ingredients,
        category_mappings=categories,
        binary_column_prefix="has_"
    )

    df_ft_recipe_ingredients = add_updated_at(df=df_ft_recipe_ingredients)

    fe = FeatureEngineeringClient()
    save_df_as_feature_table(
        df=df_ft_recipe_ingredients,
        fe=fe,
        spark=spark,
        env=args.env,
        feature_table_name=table_config.feature_table_name,
        feature_table_schema=table_config.ml_feature_schema,
        primary_keys=table_config.primary_keys,
        is_drop_existing=args.is_drop_existing
    )


def get_ingredient_data(
    spark: SparkSession,
    env: str,
    schema: Optional[str] = "mlgold"
) -> pd.DataFrame:
    custom_sql_path = SQL_DIR / "distinct_generic_ingredients.sql"
    df_distinct_ingredients = run_custom_sql(
        sql_path=custom_sql_path,  # type: ignore
        env=env,  # type: ignore
        spark=spark,
        schema=schema,  # type: ignore
        is_convert_to_pandas=True
    )
    # lower case and only alphabetical
    df_distinct_ingredients["generic_ingredient_name"] = (  # type: ignore
        df_distinct_ingredients["generic_ingredient_name"]
        .str.lower()
        .apply(lambda x: re.sub('[^a-zæåäöø ]', '', x))
    )
    return df_distinct_ingredients
