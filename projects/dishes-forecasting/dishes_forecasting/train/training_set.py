import pandas as pd
import pandera as pa
from databricks.feature_store import FeatureLookup, FeatureStoreClient
from databricks.feature_store.training_set import TrainingSet
from pyspark.sql import SparkSession

from dishes_forecasting.logger_config import logger
from dishes_forecasting.train.schemas import FlexOrders

spark = SparkSession.getActiveSession()


def create_training_data_set(
    env: str,
    company_id: str,
    train_config: dict,
    fs: FeatureStoreClient,
) -> tuple[TrainingSet, pd.DataFrame]:
    df_training_pk_target = get_training_pk_target(
        env=env,
        company_id=company_id,
        min_yyyyww=train_config["train_start_yyyyww"],
        max_yyyyww=train_config["train_end_yyyyww"],
    )

    feature_lookups = create_feature_lookups(
        env=env,
        feature_lookup_configs=train_config["feature_lookups"],
    )
    training_set = fs.create_training_set(
        spark.createDataFrame(
            df_training_pk_target.drop(columns="total_dish_quantity"),
        ),
        feature_lookups=feature_lookups,
        label="dish_ratio",
        exclude_columns=["company_id", "recipe_portion_id", "variation_id"],
    )
    return training_set, df_training_pk_target


def get_training_pk_target(
    env: str,
    company_id: str,
    min_yyyyww: int,
    max_yyyyww: int | None,
) -> pd.DataFrame:
    if max_yyyyww is None:
        # just make it a ridiculously big number
        max_yyyyww = min_yyyyww + 1000
    df_training_pk_target = (
        spark.read.table(f"{env}.mltesting.flex_orders")
        .select(
            "year",
            "week",
            "company_id",
            "variation_id",
            "dish_ratio",
            "total_dish_quantity",
        )
        .filter(f"company_id = '{company_id}'")
        .filter(f"year * 100 + week >= {min_yyyyww}")
        .filter(f"year * 100 + week <= {max_yyyyww}")
        .orderBy("year", "week")
        .toPandas()
    )
    df_training_pk_target = df_training_pk_target.dropna(subset="dish_ratio")

    try:
        df_recipe_price_ratings = FlexOrders.validate(
            df_training_pk_target,
            lazy=True,
        )
        return df_recipe_price_ratings
    except pa.errors.SchemaErrors as err:
        logger.error("Schema errors and failure cases:")
        logger.error(err.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(err.data)


def create_feature_lookups(
    env: str,
    feature_lookup_configs: list[dict],
) -> list[FeatureLookup]:
    """feature_lookup_configs have the below format:
    [
        {
            "table_name_1":
                {
                    "feature_container": "container_name",
                    "columns": {
                        primary_keys: ["a", "list", "of", "pk"],
                        feature_columns: ["a", "list", "of", "names"],
                    }
                }
        },
        {
            "table_name_2":
                {
                    "feature_container": "container_name",
                    "columns": {
                        primary_keys: ["another", "list", "of", "pk"],
                        feature_columns: ["another", "list", "of", "names"],
                    }
                }
        },
        ....
    ]

    Args:
        env (str): _description_
        feature_lookup_configs (list[dict]): _description_

    Returns:
        list[FeatureLookup]: _description_
    """
    feature_lookups = []
    for a_feature_lookup in feature_lookup_configs:
        # the key is the table name
        table_name = next(iter(a_feature_lookup.keys()))
        # get the content of the dictionary
        lookup_config = a_feature_lookup.get(table_name)
        container = lookup_config["features_container"]
        full_table_name = f"{env}.{container}.{table_name}"
        feature_lookups.append(
            FeatureLookup(
                table_name=full_table_name,
                lookup_key=lookup_config["columns"]["primary_keys"],
                feature_names=lookup_config["columns"]["feature_columns"],
            ),
        )
    return feature_lookups
