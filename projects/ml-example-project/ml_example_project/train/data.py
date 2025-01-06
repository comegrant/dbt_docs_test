import logging

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.feature_engineering.training_set import TrainingSet
from pyspark.sql import DataFrame, SparkSession

from ml_example_project.db import get_data_from_sql
from ml_example_project.paths import TRAIN_SQL_DIR
from ml_example_project.train.configs import CompanyTrainConfigs, FeatureLookUpConfig


def create_training_set(
    company_id: str,
    spark: SparkSession,
    feature_lookup_config_list: list[FeatureLookUpConfig],
    company_train_configs: CompanyTrainConfigs,
    fe: FeatureEngineeringClient | None,
) -> TrainingSet | DataFrame:
    """Create training set for specific company.

    - If feature engineering client is provided, creates the training set from FeatureLookups.
    - If feature engineering client is not provided, creates the training set from dataframes mimicking FeatureLookups.

    Parameters:
        company_id (str): Company ID.
        spark (SparkSession): Spark session.
        feature_lookup_config_list (list[FeatureLookUpConfig]): List of feature lookup configurations.
        company_train_configs (CompanyTrainConfigs): Company specific configurations.
        fe (FeatureEngineeringClient | None): Feature engineering client.

    Returns:
        TrainingSet | DataFrame: Training set, either from FeatureEngineeringClient object or dataframe.
    """
    feature_lookups, ignore_columns = create_feature_lookups(feature_lookup_config_list=feature_lookup_config_list)
    df_target = get_training_target(
        spark=spark,
        train_end_yyyyww=company_train_configs.train_end_yyyyww,
        train_start_yyyyww=company_train_configs.train_start_yyyyww,
        company_id=company_id,
    )

    if fe is not None:
        training_set = fe.create_training_set(
            df=df_target,
            feature_lookups=feature_lookups,
            label="recipe_difficulty_level_id",
            exclude_columns=ignore_columns,
        )
    else:
        # If we do not want to use feature store,
        # For example, if we want to run locally
        df_list, ignore_columns = get_feature_dataframes(
            spark=spark, feature_lookup_config_list=feature_lookup_config_list
        )
        # Merge all the tables together
        training_set = df_target
        for df in df_list:
            # The next three lines infer the common column names
            # in the two dataframes to be merged on
            columns_merged = training_set.columns
            columns_df = df.columns
            common_columns = list(set(columns_merged).intersection(columns_df))
            training_set = training_set.join(df, how="inner", on=common_columns)
        training_set = training_set.drop(*ignore_columns)

    return training_set


def get_feature_dataframes(
    spark: SparkSession,
    feature_lookup_config_list: list[FeatureLookUpConfig],
) -> tuple[list[DataFrame], list]:
    """Create list of feature dataframes and ignore columns, mimicking Databricks FeatureLookup.

    Parameters:
        spark (SparkSession): Spark session.
        feature_lookup_config_list (list[FeatureLookUpConfig]): List of feature lookup configurations.

    Returns:
        list[DataFrame]: List of feature dataframes.
        list[str]: List of columns to ingnore.
    """
    df_list = []
    ignore_columns = []
    for a_feature_lookup_config in feature_lookup_config_list:
        feature_container = a_feature_lookup_config.features_container
        feature_table_name = a_feature_lookup_config.feature_table_name
        feature_columns = a_feature_lookup_config.primary_keys + a_feature_lookup_config.feature_columns
        table_name = f"{feature_container}.{feature_table_name}"
        logging.info(f"Downloading data from {table_name} ...")
        df = spark.read.table(table_name).select(feature_columns)
        df_list.append(df)
        ignore_columns.extend(a_feature_lookup_config.exclude_in_training_set)

    ignore_columns = list(set(ignore_columns))

    return df_list, ignore_columns


def create_feature_lookups(
    feature_lookup_config_list: list[FeatureLookUpConfig],
) -> tuple[list[FeatureLookup], list[str]]:
    """Create list of feature lookups and ignore columns.

    Parameters:
        feature_lookup_config_list (list[FeatureLookUpConfig]): List of feature lookup configurations.

    Returns:
        list[FeatureLookup]: List of feature lookups.
        list[str]: List of columns to ingnore.
    """
    feature_lookups = []
    ignore_columns = []
    for a_feature_lookup_config in feature_lookup_config_list:
        # the key is the table name
        table_name = a_feature_lookup_config.feature_table_name
        container = a_feature_lookup_config.features_container
        full_table_name = f"{container}.{table_name}"
        feature_lookups.append(
            FeatureLookup(
                table_name=full_table_name,
                lookup_key=a_feature_lookup_config.primary_keys,
                feature_names=a_feature_lookup_config.feature_columns,
            ),
        )
        ignore_columns.extend(a_feature_lookup_config.exclude_in_training_set)
    ignore_columns = list(set(ignore_columns))
    return feature_lookups, ignore_columns


def get_training_target(
    spark: SparkSession,
    company_id: str,
    train_start_yyyyww: int,
    train_end_yyyyww: int | None,
) -> DataFrame:
    """
    We want to train on all recipe_id that is part of a weekly menu
    from the period {training_start_yyyyww} to {training_end_yyyyww}.
    We therefore, merge the menu with the recipe_target table to get the target set.

    Note this is the target set, which requires the resulting dataframe to have
    - the primary key of the feature table, in this case, is recipe_id
    - the target: recipe_difficulty_level_id
    """
    df = get_data_from_sql(
        spark=spark,
        sql_path=TRAIN_SQL_DIR / "training_targets.sql",
        train_start_yyyyww=train_start_yyyyww,
        train_end_yyyyww=train_end_yyyyww,
        company_id=company_id,
    )
    return df
