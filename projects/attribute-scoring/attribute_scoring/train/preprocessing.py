import logging

import pandas as pd
from attribute_scoring.train.configs import DataConfig
from sklearn.model_selection import train_test_split
from attribute_scoring.train.utils import check_target_distribution
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


def generate_sample(
    df: pd.DataFrame, target_label: str, sample_size: int
) -> pd.DataFrame:
    """
    Generate a stratified random sample from the input dataframe for training.

    Args:
        df (pd.DataFrame): The input dataframe containing features and target.
        target_label (str): The name of the target column for stratification.
        sample_size (int): The desired number of samples in the output dataframe.

    Returns:
        pd.DataFrame: A sampled dataframe with approximately `sample_size` rows, stratified by the target label.
                      If `sample_size` is None or greater than the number of rows in `df`, returns the original dataframe.
    """
    if sample_size is not None and sample_size < len(df):
        sample_fraction = sample_size / len(df)
        df_sampled, _ = train_test_split(
            df,
            test_size=1 - sample_fraction,
            stratify=df[target_label],
            random_state=10,
        )

        return pd.DataFrame(df_sampled)
    else:
        return df


def create_training_data(
    df: pd.DataFrame, target_label: str, sample_size: int = 10000
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Preprocesses the training data for model training.

    Args:
        df (pd.DataFrame): The input dataframe containing features and target.
        target_label (str): The name of the target column.
        sample_size (int, optional): The number of samples to use for training.

    Returns:
        tuple[pd.DataFrame, pd.Series]: A tuple containing the preprocessed feature dataframe and the encoded target series.
    """
    feature_columns = (
        DATA_CONFIG.recipe_features.feature_names
        + DATA_CONFIG.ingredient_features.feature_names
    )

    training_set = df[feature_columns + [target_label]]
    training_set = training_set.dropna(subset=[target_label])  # type: ignore
    training_set = training_set.drop_duplicates()

    training_sample = generate_sample(training_set, target_label, sample_size)

    training_data = training_sample.drop(target_label, axis=1)
    target = pd.Series(training_sample[target_label])

    logging.info(check_target_distribution(target))

    target_encoded, _ = encode_labels(target)

    return training_data, target_encoded
