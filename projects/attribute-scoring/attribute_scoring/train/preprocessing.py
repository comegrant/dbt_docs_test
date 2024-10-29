import logging

import pandas as pd
from attribute_scoring.common import Args
from attribute_scoring.train.config import DataConfig
from attribute_scoring.train.data import create_training_data, get_feature_lookups, get_raw_data
from attribute_scoring.train.utils import check_target_distribution
from constants.companies import get_company_by_code
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient
from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import LabelEncoder

DATA_CONFIG = DataConfig()


def encode_labels(target: pd.Series) -> tuple[pd.Series, LabelEncoder]:
    """
    Encode target labels using LabelEncoder.

    Args:
        y_train (pd.Series): The target variable series to encode.

    Returns:
        tuple[pd.Series, LabelEncoder]: A tuple containing the encoded target and label encoder.
    """
    label_encoder = LabelEncoder()
    target_encoded = label_encoder.fit_transform(target)
    return target_encoded, label_encoder


def prepare_training_data(
    args: Args, fe: FeatureEngineeringClient, spark: DatabricksSession
) -> tuple[pd.DataFrame, pd.Series, any]:
    """Prepares data for model training.

    Prepares the training data by fetching raw data, applying feature lookups,
    and encoding the target labels.

    Returns:
        tuple[pd.DataFrame, pd.Series, any]:
            - data (pd.DataFrame): Processed feature data for training.
            - target_encoded (pd.Series): Encoded target variable.
            - training_set (any): Training set object.
    """
    logging.info("Fetching data...")
    company_id = get_company_by_code(args.company).company_id
    target_label = args.target

    raw_data = get_raw_data(env=args.env, company_id=company_id, target_label=target_label, spark=spark)

    lookup = get_feature_lookups(
        feature_table=DATA_CONFIG.feature_table,
        feature_names=DATA_CONFIG.feature_names,
        primary_keys=DATA_CONFIG.primary_keys,
    )

    logging.info("Creating training set...")
    training_df, training_set = create_training_data(
        data=raw_data,
        fe=fe,
        feature_lookup=lookup,
        target_label=target_label,
        excluded_columns=DATA_CONFIG.excluded_columns,
        spark=spark,
    )

    training_df = training_df.drop_duplicates()
    logging.info(f"Created training set with {training_df.shape[0]} rows.")

    data = training_df.drop([target_label], axis=1)
    target = training_df[target_label]
    logging.info(check_target_distribution(target))

    target_encoded, _ = encode_labels(target)

    return data, target_encoded, training_set


def oversample_data(
    data: pd.DataFrame, target: pd.Series, sampling_ratio: float = 0.42
) -> tuple[pd.DataFrame, pd.Series]:
    """Performs oversampling of the minority class using SMOTE to balance the dataset.

    Args:
        data (pd.DataFrame): The feature data.
        target (pd.Series): The target variable.
        sampling_ratio (float, optional): The desired ratio of minority class samples to majority class samples.

    Returns:
        tuple[pd.DataFrame, pd.Series]: The oversampled data and target.
    """
    oversample = SMOTE(sampling_strategy=sampling_ratio, random_state=42)
    data_smote, target_smote = oversample.fit_resample(data, target)

    return data_smote, target_smote
