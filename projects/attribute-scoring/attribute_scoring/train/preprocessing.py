import logging

import pandas as pd
from attribute_scoring.common import Args
from attribute_scoring.train.configs import DataConfig
from attribute_scoring.train.data import create_training_data, get_feature_lookups, get_training_data
from attribute_scoring.train.utils import check_target_distribution
from constants.companies import get_company_by_code
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.training_set import TrainingSet
from sklearn.preprocessing import LabelEncoder

DATA_CONFIG = DataConfig()


def encode_labels(target: pd.Series) -> tuple[pd.Series, LabelEncoder]:
    """
    Encode target labels using LabelEncoder.

    Args:
        target (pd.Series): The target variable series to encode.

    Returns:
        tuple[pd.Series, LabelEncoder]: A tuple containing the encoded target and label encoder.
    """
    label_encoder = LabelEncoder()
    target_encoded = pd.Series(label_encoder.fit_transform(target), index=target.index)
    return target_encoded, label_encoder


def prepare_training_data(
    args: Args, fe: FeatureEngineeringClient, spark: DatabricksSession
) -> tuple[pd.DataFrame, pd.Series, TrainingSet]:
    """Prepares data for model training.

    Prepares the training data by fetching raw data, applying feature lookups,
    and encoding the target labels.

    Args:
        args (Args): Configuration arguments.
        fe (FeatureEngineeringClient): Feature engineering client.
        spark (DatabricksSession): The Spark session to use for querying.

    Returns:
        tuple[pd.DataFrame, pd.Series, any]:
            - data (pd.DataFrame): Processed feature data for training.
            - target_encoded (pd.Series): Encoded target variable.
            - training_set (any): Training set object.
    """
    logging.info("Fetching data...")
    company_id = get_company_by_code(args.company).company_id
    target_label = args.target

    if target_label is None:
        raise ValueError("target_label cannot be None")

    df = get_training_data(spark=spark, company_id=company_id, target_label=target_label)

    lookup = get_feature_lookups(
        lookup_recipe=DATA_CONFIG.recipe_feature_lookup,
        lookup_ingredients=DATA_CONFIG.ingredient_feature_lookup,
    )

    logging.info("Creating training set...")
    training_df, training_set = create_training_data(
        df=df,
        fe=fe,
        feature_lookup=lookup,
        target_label=target_label,
        excluded_columns=DATA_CONFIG.excluded_columns,
    )

    training_df = training_df.drop_duplicates()
    logging.info(f"Created training set with {training_df.shape[0]} rows.")

    data = training_df.drop([target_label], axis=1)
    target = pd.Series(training_df[target_label])
    logging.info(check_target_distribution(target))

    target_encoded, _ = encode_labels(target)

    return data, target_encoded, training_set
