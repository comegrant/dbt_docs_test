import pandas as pd
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from attribute_scoring.train.config import FeatureLookupConfig


def get_raw_data(
    env: str,
    target_label: str,
    company_id: str,
    spark: DatabricksSession,
) -> pd.DataFrame:
    """
    Fetches raw target data from a Databricks SQL table.

    Args:
        env (str): The environment (e.g., 'dev', 'prod') to fetch data from.
        target_label (str): The name of the target column.
        company_id (str): The ID of the company to filter data for.
        spark (DatabricksSession): The Spark session to use for querying.

    Returns:
        pd.DataFrame: A DataFrame containing the recipe_id and target label.
    """
    target_query = f"""
        select
            recipe_id,
            recipe_portion_id,
            language_id,
            {target_label}
        from {env}.mlfeatures.ft_ml_recipes
        where company_id = '{company_id}'
    """

    targets = spark.sql(target_query).toPandas()

    return targets


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
    data: pd.DataFrame,
    fe: FeatureEngineeringClient,
    feature_lookup: list[FeatureLookup],
    target_label: str,
    excluded_columns: list[str],
    spark: DatabricksSession,
) -> tuple[pd.DataFrame, any]:
    """
    Creates a training dataset from the raw data and provided feature lookups.

    Args:
        data (pd.DataFrame): The raw input data as a Pandas DataFrame.
        fe (FeatureEngineeringClient): A feature engineering client.
        feature_lookup (list[FeatureLookup]): A list of FeatureLookup object(s).
        target_label (str): The target column to predict.
        excluded_columns (list[str]): Columns to exclude from the final dataset.
        spark (DatabricksSession): A Spark session.

    Returns:
        tuple[pd.Dataframe, object]: A tuple containig the training data as a dataframe and the training set object.
    """
    training_set = fe.create_training_set(
        df=spark.createDataFrame(data),
        feature_lookups=feature_lookup,
        label=target_label,
        exclude_columns=excluded_columns,
    )

    training_df = training_set.load_df().toPandas()

    return training_df, training_set
