from typing import Literal

from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel
from pyspark.sql import SparkSession

from ml_feature_store.common.data import get_data_from_catalog, save_df_as_feature_table
from ml_feature_store.feature_tables import ft_ml_recipes_configs
from ml_feature_store.ft_ml_recipes.feature_generator import (
    convert_columns_to_int,
    generate_boolean_taxonomy_attributes,
    generate_mean_cooking_time,
)


class Args(BaseModel):
    env: Literal["dev", "test", "prod"]


def build_feature_table(args: Args, spark: SparkSession) -> None:
    table_config = ft_ml_recipes_configs

    df = get_data_from_catalog(
        spark=spark,
        env=args.env,
        table_name=table_config.dbt_model_name,
        schema=table_config.dbt_model_schema,
        is_convert_to_pandas=True,
    )

    df = generate_mean_cooking_time(df)
    df = generate_boolean_taxonomy_attributes(df)
    df = convert_columns_to_int(df)
    fe = FeatureEngineeringClient()

    save_df_as_feature_table(
        spark=spark,
        df=df,
        fe=fe,
        env=args.env,
        feature_table_name=table_config.feature_table_name,
        feature_table_schema=table_config.ml_feature_schema,
        primary_keys=table_config.primary_keys,
    )
