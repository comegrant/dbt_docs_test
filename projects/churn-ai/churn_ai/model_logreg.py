from locale import normalize
import pickle
import pandas as pd
from prep import Prep
from model import Model
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support as score


class ModelLogReg(Model):
    """Model: Logistic Regression"""

    categorical_features = ["snapshot_status", "planned_delivery"]
    numerical_features = [
        "number_of_total_orders",
        "weeks_since_last_delivery",
        "customer_since_weeks",
        "weeks_since_last_complaint",
    ]
    prediction_label = ["forecast_status"]
    customer_id_label = "agreement_id"
    predictions_label = "predicted_churned"

    training_features = categorical_features + numerical_features + prediction_label
    prediction_features = (
        categorical_features + numerical_features + [customer_id_label]
    )

    df_x = None
    df_y = None
    df_x_val = None
    df_y_val = None
    model = None

    def __init__(self, data=None, config=None, probability_threshold=0.3):
        """
        :param data: Pandas Dataframe (merged dataset coming from gen.py)
        :probability_threshold: Probability threshold for model estimator
        :param config:
        """
        self.df = data
        self.config = config
        self.probability_threshold = probability_threshold
        self.forecast_weeks = self.config["snapshot"]["forecast_weeks"]

    def prep_training(self, validation_split_months=2):
        """
        Data preprocessing for model training
        :validation_split: Validation split in months
        :return:
        """
        # Check to make sure snapshot_date has right type of data
        # If merging snapshot_* files locally to create full_training_snapshot, then headers can come in the merged file
        indices_to_drop = self.df[self.df.snapshot_date.str.len() > 10].index
        self.df.drop(indices_to_drop, axis=0, inplace=True)
        self.df["snapshot_date"] = pd.to_datetime(self.df["snapshot_date"])
        max_df_date = self.df.snapshot_date.max()
        dt_from = max_df_date - pd.DateOffset(months=validation_split_months)
        print(
            "Removing data prior to forecast_weeks. Data size before: "
            + str(self.df.shape[0])
        )  # Is it forecast weeks or validation_split_months = 2?
        self.df = self.df[self.df["snapshot_date"] <= dt_from]
        print("Data size after: " + str(self.df.shape[0]))

        validation_split_date = dt_from - pd.DateOffset(months=self.forecast_weeks)
        # Train / Test set
        print("Train / test dataset with snapshot date <:", validation_split_date)
        self.df_x, self.df_y = Prep().prep_training(
            pd.DataFrame(self.df[self.df.snapshot_date < validation_split_date].copy()),
            self.training_features,
            categ_columns=self.categorical_features,
            drop_nan=True,
            label_column="forecast_status",
        )

        # Validation set
        print("Validation dataset with snapshot date >=:", validation_split_date)
        self.df_x_val, self.df_y_val = Prep().prep_training(
            self.df[self.df.snapshot_date >= validation_split_date].copy(),
            self.training_features,
            categ_columns=self.categorical_features,
            drop_nan=True,
            label_column="forecast_status",
        )

        print(
            "train-test data size prior to: ",
            validation_split_date,
            " is: " + str(self.df_x.shape),
        )
        print(
            "validation data size after: ",
            validation_split_date,
            " is: " + str(self.df_x_val.shape),
        )

    def fit(self, save_model_filename=None):
        """
        Main function for model fit
        :save_model_filename: Default None. If given model will be saved to given path/name (pickle format)
        :return:
        """
        print("Fitting/Training the model..")

        x_train, x_test, y_train, y_test = train_test_split(
            self.df_x, self.df_y, test_size=0.2, random_state=42
        )

        # Dimensions of the train-test set
        print(x_train.shape, x_test.shape)

        # Class distribution between train-test set
        print(
            "Class distribution in training set:", y_train.value_counts(normalize=True)
        )
        print("Class distribution in test set:", y_test.value_counts(normalize=True))

        self.model = LogisticRegression(random_state=0, max_iter=1000)
        self.model.fit(x_train, y_train)

        print("Training features columns:" + str(x_train.columns))
        y_val_pred = (
            self.model.predict_proba(self.df_x_val)[:, 1] >= self.probability_threshold
        ).astype(int)

        precision, recall, fscore, _ = score(self.df_y_val, y_val_pred, average="macro")
        print("precision: {}".format(precision))
        print("recall: {}".format(recall))
        print("f1-score: {}".format(fscore))

        if save_model_filename:
            print("Saving model to: " + save_model_filename)
            with open(save_model_filename, "wb") as file:
                pickle.dump(self.model, file)
            print("Model saved!")

        return True

    def predict(self, df, model_obj=None, model_filename=None):
        """
        Model predictions
        :param df: dataframe with customer for predictions
        :param model_filename: model filename / path
        :return: Dataframe with predictions
        """

        df_predict = Prep().prep_prediction(
            df=df,
            columns_to_keep=self.prediction_features,
            categ_columns=self.categorical_features,
        )

        if model_filename != None:
            print("Loading model from file: " + model_filename)
            model = pickle.load(open(model_filename, "rb"))
        else:
            model = model_obj

        df_in = df_predict.drop(self.customer_id_label, axis=1)

        print("Predict features columns:" + str(df_predict.columns))

        pred_proba = model.predict_proba(df_in)

        # predictions_score = [
        #    p[1] if p[1] >= self.probability_threshold else 1 - p[1]
        #    for p in pred_proba[
        #        :,
        #    ]
        # ]

        # predictions = (pred_proba[:, 1] >= self.probability_threshold).astype(int)

        # df_predict[self.predictions_label] = predictions
        # df_predict["score"] = predictions_score
        # return df_predict

        # Original
        # predictions_score = [p[1] if p[1] >= self.probability_threshold else 1 - p[1] for p in pred_proba[:,]]
        predictions_score = [
            p[1]
            for p in pred_proba[
                :,
            ]
        ]

        # predictions = (pred_proba[:,1] >= self.probability_threshold).astype(int) #Deactivte churn_flag of 0/1

        # df_predict[self.predictions_label] = predictions
        df_predict["score"] = predictions_score
        df_predict["model_type"] = "original_pred_proba"
        return df_predict
