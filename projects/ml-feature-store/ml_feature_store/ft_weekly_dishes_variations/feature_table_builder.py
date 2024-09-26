from typing import Literal

from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel
from pyspark.sql import SparkSession

from ml_feature_store.common.data import add_updated_at, get_data_from_catalog, save_df_as_feature_table
from ml_feature_store.feature_tables import ft_weekly_dishes_variations_configs


class Args(BaseModel):
    env: Literal["dev", "test", "prod"]


def build_feature_table(args: Args, spark: SparkSession) -> None:
    table_config = ft_weekly_dishes_variations_configs
    df = get_data_from_catalog(
        spark=spark,
        env=args.env,
        table_name=table_config.dbt_model_name,
        schema=table_config.dbt_model_schema,
        is_convert_to_pandas=True
    )

    df = df.drop_duplicates(subset=table_config.primary_keys)

    # Add current timestamp
    df = add_updated_at(df=df)

    fe = FeatureEngineeringClient()
    save_df_as_feature_table(
        spark=spark,
        df=df,
        fe=fe,
        env=args.env,
        feature_table_name=table_config.feature_table_name,
        feature_table_schema=table_config.ml_feature_schema,
        primary_keys=table_config.primary_keys
    )
