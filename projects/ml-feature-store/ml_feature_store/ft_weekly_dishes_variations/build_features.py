from datetime import datetime
from typing import Literal

import pytz
from databricks.feature_engineering import FeatureEngineeringClient
from pydantic import BaseModel
from pyspark.sql import SparkSession

from ml_feature_store.common.data import get_data_from_catalog, save_df_as_feature_table
from ml_feature_store.feature_tables import ft_weekly_dishes_variations_configs


class Args(BaseModel):
    env: Literal["dev", "test", "prod"]


def build_features(args: Args, spark: SparkSession) -> None:
    table_config = ft_weekly_dishes_variations_configs
    df = get_data_from_catalog(
        spark=spark,
        env=args.env,
        table_name=table_config.dbt_model_name,
        schema=table_config.dbt_model_schema,
        is_convert_to_pandas=True
    )
    cet_tz = pytz.timezone("CET")
    if "updated_at" not in df.columns:
        # Add a column
        df = df.assign(updated_at=datetime.now(tz=cet_tz))
    else:
        # Update the column with the current time stamp
        df.loc[:, "updated_at"] = datetime.now(tz=cet_tz)
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
