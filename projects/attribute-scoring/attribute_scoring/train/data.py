import pandas as pd
from attribute_scoring.db import get_data_from_sql
from attribute_scoring.paths import TRAIN_SQL_DIR
from attribute_scoring.train.configs import FeatureLookupConfig
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.feature_engineering.training_set import TrainingSet
from pyspark.sql import DataFrame


def get_training_data(
    spark: DatabricksSession,
    company_id: str,
    target_label: str,
) -> DataFrame:
    """Fetches training data from a Databricks SQL table.

    Args:
        spark (DatabricksSession): The Spark session to use for querying.
        company_id (str): The ID of the company to filter data for.
        target_label (str): The name of the target column.

    Returns:
        DataFrame: A DataFrame containing the recipe_id and target label.
    """
    df = get_data_from_sql(
        spark=spark,
        sql_path=TRAIN_SQL_DIR / "training_targets.sql",
        company_id=company_id,
        target_label=target_label,
    )

    return df


def get_feature_lookups(
    lookup_recipe: FeatureLookupConfig,
    lookup_ingredients: FeatureLookupConfig,
) -> list:
    """
    Creates a list of FeatureLookup objects for feature engineering.

    Args:
        lookup_recipe: feature_table, feature_names, and primary_keys from recipe feature table
        lookup_ingredients: feature_table, feature_names, and primary_keys from ingredients feature table

    Returns:
        list: A list containing a single FeatureLookup object.
    """
    feature_lookup = [
        FeatureLookup(
            table_name=lookup_recipe.feature_table,
            feature_names=lookup_recipe.feature_names,
            lookup_key=lookup_recipe.primary_keys,
        ),
        FeatureLookup(
            table_name=lookup_ingredients.feature_table,
            feature_names=lookup_ingredients.feature_names,
            lookup_key=lookup_ingredients.primary_keys,
        ),
    ]

    return feature_lookup


def create_training_data(
    df: DataFrame,
    fe: FeatureEngineeringClient,
    feature_lookup: list[FeatureLookup],
    target_label: str,
    excluded_columns: list[str],
) -> tuple[pd.DataFrame, TrainingSet]:
    """
    Creates a training dataset from the raw data and provided feature lookups.

    Args:
        data (pd.DataFrame): The raw input data as a Pandas DataFrame.
        fe (FeatureEngineeringClient): A feature engineering client.
        feature_lookup (list[FeatureLookup]): A list of FeatureLookup object(s).
        target_label (str): The target column to predict.
        excluded_columns (list[str]): Columns to exclude from the final dataset.

    Returns:
        tuple[pd.Dataframe, object]: A tuple containig the training data as a dataframe and the training set object.
    """
    training_set = fe.create_training_set(
        df=df,
        feature_lookups=feature_lookup,  # type: ignore
        label=target_label,
        exclude_columns=excluded_columns,
    )

    training_df = training_set.load_df().toPandas()

    return training_df, training_set
