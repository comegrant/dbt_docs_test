import pandas as pd
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup


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
            {target_label}
        from {env}.mlfeatures.ft_ml_recipes
        where company_id = '{company_id}'
    """

    targets = spark.sql(target_query).toPandas()

    return targets


def get_feature_lookups(
    feature_table: str,
    feature_names: list[str],
    primary_keys: list[str],
) -> list:
    """
    Creates a list of FeatureLookup objects for feature engineering.

    Args:
        feature_table (str): The name of the feature table.
        feature_names (list[str]): A list of feature column names to include.
        primary_keys (list[str]): A list of primary key column names for lookup.

    Returns:
        list: A list containing a single FeatureLookup object.
    """
    feature_lookup = [
        FeatureLookup(
            table_name=feature_table,
            feature_names=feature_names,
            lookup_key=primary_keys,
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
