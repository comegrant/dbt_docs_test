import logging
from typing import ClassVar

import pandas as pd

logger = logging.getLogger(__name__)


class Preprocessor:
    CATEGORICAL_FEATURES: ClassVar[list[str]] = ["snapshot_status", "planned_delivery"]
    NUMERICAL_FEATURES: ClassVar[list[str]] = [
        "number_of_total_orders",
        "weeks_since_last_delivery",
        "customer_since_weeks",
        "weeks_since_last_complaint",
    ]
    PREDICTION_LABEL: ClassVar[list[str]] = ["forecast_status"]
    CUSTOMER_ID_LABEL: str = "agreement_id"
    TRAINING_FEATURES: ClassVar[list[str]] = (
        CATEGORICAL_FEATURES + NUMERICAL_FEATURES + PREDICTION_LABEL
    )
    PREDICTION_FEATURES: ClassVar[list[str]] = (
        CATEGORICAL_FEATURES + NUMERICAL_FEATURES + [CUSTOMER_ID_LABEL]
    )

    @classmethod
    def normalize_df(cls: type["Preprocessor"], df: pd.DataFrame) -> pd.DataFrame:
        """Normalize numerical features in the dataframe."""
        numerical_cols = [col for col in df.columns if col in cls.NUMERICAL_FEATURES]
        df[numerical_cols] = (df[numerical_cols] - df[numerical_cols].min()) / (
            df[numerical_cols].max() - df[numerical_cols].min()
        )
        df[numerical_cols] = df[numerical_cols].fillna(0)
        return df

    @classmethod
    def handle_categorical_features(
        cls: type["Preprocessor"],
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Convert categorical features into one-hot encoding."""
        for feature in cls.CATEGORICAL_FEATURES:
            dummies = pd.get_dummies(df[feature], prefix=feature)
            df = pd.concat([df, dummies], axis=1).drop(feature, axis=1)

        for col in [
            "snapshot_status_active",
            "snapshot_status_freezed",
            "snapshot_status_churned",
            "planned_delivery_True",
            "planned_delivery_False",
        ]:
            if col not in df.columns:
                df[col] = 0
        return df

    @classmethod
    def prep(
        cls: type["Preprocessor"],
        df: pd.DataFrame,
        columns_to_keep: list,
        drop_nan: bool = False,
        no_delivery_churned_weeks: int = 4,
    ) -> pd.DataFrame:
        if df.empty:
            logger.info("Provided dataframe is empty.")
            return df

        logger.info(f"Dataset size before preprocessing: {df.shape[0]}")
        df = df[df["weeks_since_last_delivery"] < no_delivery_churned_weeks]
        df = df[df["weeks_since_last_delivery"] >= 0]
        logger.info(df.columns)
        df["category"] = df["category"].fillna(value="0")
        df = df[columns_to_keep]
        df = cls.handle_categorical_features(df)

        if drop_nan:
            df = df.dropna()
            logger.info(f"Total NaN rows dropped: {df.shape[0]}")

        df = cls.normalize_df(df)

        return df.reindex(sorted(df.columns), axis=1)

    @classmethod
    def prep_prediction(
        cls: type["Preprocessor"],
        df: pd.DataFrame,
        columns_to_keep: list = PREDICTION_FEATURES,
        customer_id_label: str = "agreement_id",
        drop_nan: bool = True,
    ) -> pd.DataFrame:
        logger.info(df.columns)
        df_prep = cls.prep(df, columns_to_keep=columns_to_keep, drop_nan=drop_nan)
        df_prep[customer_id_label] = df[customer_id_label]
        return df_prep

    @classmethod
    def prep_training(
        cls: type["Preprocessor"],
        df: pd.DataFrame,
        columns_to_keep: list = TRAINING_FEATURES,
        drop_nan: bool = False,
        label_column: str = "forecast_status",
        no_delivery_churned_weeks: int = 4,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        df.loc[df[label_column] == "freezed", label_column] = "active"
        df.loc[df[label_column] == "active", label_column] = 0
        df.loc[df[label_column] == "churned", label_column] = 1
        logger.info(df[label_column])

        df.loc[:, label_column] = df[label_column].astype(int)
        df_prep = cls.prep(
            df,
            columns_to_keep=columns_to_keep,
            drop_nan=drop_nan,
            no_delivery_churned_weeks=no_delivery_churned_weeks,
        )
        df_y = df_prep.pop(label_column).astype(int)
        return df_prep, df_y

    @classmethod
    def filter_data_by_date(
        cls: type["Preprocessor"],
        data: pd.DataFrame,
        date_column: str,
        before_date: pd.Timestamp,
        after_date: pd.Timestamp = None,
    ) -> pd.DataFrame:
        """
        Filter the data based on the snapshot_date being before a certain date,
        and optionally after another date.
        """
        if after_date:
            return data[
                (data[date_column] < before_date) & (data[date_column] >= after_date)
            ]
        return data[data[date_column] < before_date]

    @classmethod
    def prep_training_data(
        cls: type["Preprocessor"],
        data: pd.DataFrame,
        validation_split_months: int = 2,
        training_features: list[str] | None = None,
        forecast_weeks: int = 4,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Data preprocessing for model training.
        :param validation_split_months: Validation split in months.
        :param training_features: Features to include in the training data.
        :param forecast_weeks: Number of weeks for forecasting.
        :return: Tuple containing training features, training labels, validation features, and validation labels.
        """
        date_column = "snapshot_date"

        if training_features is None:
            training_features = cls.TRAINING_FEATURES

        data[date_column] = pd.to_datetime(data[date_column])
        data.dropna(subset=["forecast_status"], inplace=True)

        max_date = data[date_column].max()
        validation_split_date = max_date - pd.DateOffset(months=validation_split_months)
        forecast_split_date = validation_split_date - pd.DateOffset(
            weeks=forecast_weeks,
        )

        logger.info(
            f"Validation split date: {validation_split_date}, forecast split date: {forecast_split_date}",
        )

        logger.info(
            f"Data size before removing data prior to forecast_weeks: {data.shape[0]}",
        )
        training_set = cls.filter_data_by_date(
            data,
            date_column,
            validation_split_date,
        )
        logger.info(
            f"Training set size: {len(training_set)} with max date: {training_set[date_column].max()}",
        )

        validation_set = cls.filter_data_by_date(
            data,
            date_column,
            max_date,
            validation_split_date,
        )
        logger.info(
            f"Validation set size: {len(validation_set)} with max date: {validation_set[date_column].max()}",
        )

        if validation_set.empty:
            raise ValueError(
                f"Validation set is empty! Validation split is set to {validation_split_date}. Max date is {max_date}",
            )

        # Preprocess training and validation sets
        df_x_train, df_y_train = cls.prep_training(
            training_set,
            training_features,
            drop_nan=True,
            label_column="forecast_status",
        )
        df_x_val, df_y_val = cls.prep_training(
            validation_set,
            training_features,
            drop_nan=True,
            label_column="forecast_status",
        )

        if df_x_val.empty:
            raise ValueError(
                f"Validation set is empty! Max date for training set is {df_x_train[date_column].max()}",
            )

        logger.info(
            f"Train data size: {df_x_train.shape}, Validation data size: {df_x_val.shape}",
        )

        return df_x_train, df_y_train, df_x_val, df_y_val
