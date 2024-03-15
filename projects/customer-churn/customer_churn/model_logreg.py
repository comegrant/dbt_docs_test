import logging
import pickle
from pathlib import Path
from typing import ClassVar

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split

from customer_churn.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


class ModelLogReg:
    """Model: Logistic Regression"""

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
    PREDICTION_FETURES: ClassVar[list[str]] = (
        CATEGORICAL_FEATURES + NUMERICAL_FEATURES + [CUSTOMER_ID_LABEL]
    )

    def __init__(
        self,
        data: pd.DataFrame,
        config: dict,
        probability_threshold: float = 0.3,
    ):
        """
        :param data: Pandas Dataframe (merged dataset coming from gen.py)
        :probability_threshold: Probability threshold for model estimator
        :param config:
        """
        self.df = data
        self.config = config
        self.probability_threshold = probability_threshold
        self.forecast_weeks = self.config["snapshot"]["forecast_weeks"]

        self.df_x = None
        self.df_y = None
        self.df_x_val = None
        self.df_y_val = None
        self.model = None

    def prep_training(
        self,
        validation_split_months: int = 2,
        num_indeces_drop: int = 10,
    ) -> None:
        """
        Data preprocessing for model training
        :validation_split: Validation split in months
        :return:
        """
        # Check to make sure snapshot_date has right type of data
        # If merging snapshot_* files locally to create full_training_snapshot, then headers can come in the merged file
        indices_to_drop = self.df[
            self.df.snapshot_date.str.len() > num_indeces_drop
        ].index
        self.df.drop(indices_to_drop, axis=0, inplace=True)
        self.df["snapshot_date"] = pd.to_datetime(self.df["snapshot_date"])
        max_df_date = self.df.snapshot_date.max()
        dt_from = max_df_date - pd.DateOffset(months=validation_split_months)
        logger.info(
            "Removing data prior to forecast_weeks. Data size before: "
            + str(self.df.shape[0]),
        )  # Is it forecast weeks or validation_split_months = 2?
        self.df = self.df[self.df["snapshot_date"] <= dt_from]
        logger.info("Data size after: %s" % str(self.df.shape[0]))

        validation_split_date = dt_from - pd.DateOffset(months=self.forecast_weeks)
        # Train / Test set
        logger.info(
            "Train / test dataset with snapshot date <: %s" % validation_split_date,
        )
        self.df_x, self.df_y = Preprocessor().prep_training(
            pd.DataFrame(self.df[self.df.snapshot_date < validation_split_date].copy()),
            self.TRAINING_FEATURES,
            drop_nan=True,
            label_column="forecast_status",
        )

        # Validation set
        logger.info(
            "Validation dataset with snapshot date >=: %s" % validation_split_date,
        )
        self.df_x_val, self.df_y_val = Preprocessor().prep_training(
            self.df[self.df.snapshot_date >= validation_split_date].copy(),
            self.TRAINING_FEATURES,
            drop_nan=True,
            label_column="forecast_status",
        )

        logger.info(
            f"train-test data size prior to: {validation_split_date} is: {self.df_x.shape!s}",
        )
        logger.info(
            f"validation data size after: {validation_split_date} is: {self.df_x_val.shape!s}",
        )

    def fit(self, save_model_filename: str | None = None) -> bool:
        """
        Main function for model fit
        :save_model_filename: Default None. If given model will be saved to given path/name (pickle format)
        :return:
        """
        logger.info("Fitting/Training the model..")

        x_train, x_test, y_train, y_test = train_test_split(
            self.df_x,
            self.df_y,
            test_size=0.2,
            random_state=42,
        )

        # Dimensions of the train-test set
        logger.info(f"train {x_train.shape}, test {x_test.shape}")

        # Class distribution between train-test set
        logger.info(
            f"Class distribution in training set: {y_train.value_counts(normalize=True)}",
        )
        logger.info(
            "Class distribution in test set: %s" % y_test.value_counts(normalize=True),
        )

        self.model = LogisticRegression(random_state=0, max_iter=1000)
        self.model.fit(x_train, y_train)

        logger.info(f"Training features columns: {x_train.columns!s}")
        y_val_pred = (
            self.model.predict_proba(self.df_x_val)[:, 1] >= self.probability_threshold
        ).astype(int)

        precision, recall, fscore, _ = score(self.df_y_val, y_val_pred, average="macro")
        logger.info(f"precision: {precision}")
        logger.info(f"recall: {recall}")
        logger.info(f"f1-score: {fscore}")

        if save_model_filename:
            logger.info("Saving model to: " + save_model_filename)
            with Path.open(save_model_filename, "wb") as file:
                pickle.dump(self.model, file)
            logger.info("Model saved!")

        return True

    def predict(
        self,
        df: pd.DataFrame,
        model_obj: LogisticRegression | None = None,
        model_filename: str | None = None,
    ) -> pd.DataFrame:
        """
        Model predictions
        :param df: dataframe with customer for predictions
        :param model_filename: model filename / path
        :return: Dataframe with predictions
        """

        df_predict = Preprocessor().prep_prediction(
            df=df,
            columns_to_keep=self.PREDICTION_FETURES,
        )

        if model_filename is not None:
            logger.info(f"Loading model from file: {model_filename}")
            model = pickle.load(Path.open(model_filename, "rb"))
        else:
            model = model_obj

        df_in = df_predict.drop(self.CUSTOMER_ID_LABEL, axis=1)

        logger.info(f"Predict features columns: {df_predict.columns!s}")

        pred_proba = model.predict_proba(df_in)

        #    for p in pred_proba[
        #        :,

        # Original
        predictions_score = [p[1] for p in pred_proba[:,]]

        df_predict["score"] = predictions_score
        df_predict["model_type"] = "original_pred_proba"
        return df_predict
