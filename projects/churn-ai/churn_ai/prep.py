import numpy as np
import pandas as pd


class Prep:
    @staticmethod
    def prep_prediction(
        df=pd.DataFrame(),
        columns_to_keep=[],
        categ_columns=[],
        customer_id_label="agreement_id",
        drop_nan=True,
    ):
        """
        Main function for data preprocessing before model prediction
        :param df:
        :param columns_to_keep:
        :param categ_columns:
        :param customer_id_label:
        :param drop_nan:
        :return: Preprocessed dataframe if df given
        """
        if df.empty:
            return

        df_prep = Prep().prep(
            df,
            columns_to_keep=columns_to_keep,
            categ_columns=categ_columns,
            drop_nan=drop_nan,
        )
        # Normalization
        df_prep_normalized = (df_prep.astype(int) - df_prep.min().astype(int)) / (
            df_prep.max().astype(int) - df_prep.min().astype(int)
        )
        df_prep_normalized = df_prep_normalized.fillna(0)
        df_prep_normalized[customer_id_label] = df_prep[customer_id_label]
        return df_prep_normalized

    @staticmethod
    def prep(
        df,
        columns_to_keep=[],
        drop_nan=False,
        categ_columns=[],
        no_delivery_churned_weeks=4,
    ):
        """
        Preproc df
        :columns_to_keep: List of features columns that should be returned
        :drop_nan: Drop NaN rows
        :categ_columns: List of categorical columns
        :label_column: Label feature (returned as df_y)
        :no_delivery_churned_weeks: Removed customers for training that already churned
        :return:
        """
        print("Dataset size before preprocessing: " + str(df.shape[0]))
        df_prep = df.copy()
        # This part of code/method is most likey geared for TRAINING THE MODEL with specific features.
        # Also for prediction? The prediction dataset also needs to be transformed esp snapshot_status + planned_delivery colns

        # Remove customers that already churned or have -1 weeks_since_last_delivery
        print(
            "Dataset size before weeks_since_last_delivery delete: "
            + str(df_prep.shape[0])
        )
        df_prep.weeks_since_last_delivery = df_prep.weeks_since_last_delivery.astype(
            int
        )  # Adding this as the next line breaks otherwise
        df_prep = df_prep[
            df_prep["weeks_since_last_delivery"] < no_delivery_churned_weeks
        ]
        print(
            "Dataset size after weeks_since_last_delivery delete: "
            + str(df_prep.shape[0])
        )
        df_prep = df_prep[df_prep.weeks_since_last_delivery >= 0]
        print(
            "Dataset size after removing -1 weeks_since_last_delivery: "
            + str(df_prep.shape[0])
        )  # Basically all entries containing '-1' as value

        # Fill 0 to complaints.category (customers that had no complaints)
        df_prep.category = df_prep[["category"]].fillna(value="0")

        df_prep = df_prep[columns_to_keep]

        # Categorical features
        # df_prep = pd.get_dummies(df_prep, columns=categ_columns)

        df_prep["snapshot_status_active"] = np.where(
            df_prep["snapshot_status"] == "active", 1.0, 0.0
        )
        df_prep["snapshot_status_freezed"] = np.where(
            df_prep["snapshot_status"] == "freezed", 1.0, 0.0
        )
        df_prep["planned_delivery_False"] = np.where(
            df_prep["planned_delivery"] == False, 1.0, 0.0
        )
        df_prep["planned_delivery_True"] = np.where(
            df_prep["planned_delivery"] == True, 1.0, 0.0
        )
        df_prep = df_prep.drop(["snapshot_status", "planned_delivery"], axis=1)

        df_prep.snapshot_status_active = df_prep[["snapshot_status_active"]].fillna(
            value=0.0
        )
        df_prep.snapshot_status_freezed = df_prep[["snapshot_status_freezed"]].fillna(
            value=0.0
        )
        df_prep.planned_delivery_False = df_prep[["planned_delivery_False"]].fillna(
            value=0.0
        )
        df_prep.planned_delivery_True = df_prep[["planned_delivery_True"]].fillna(
            value=0.0
        )

        # Drop NaN / missing values
        if drop_nan:
            df_nan_free = df_prep.dropna()
            only_nan = df_prep[np.invert(df_prep.index.isin(df_nan_free.index))]
            print("Total NaN rows do be dropped: " + str(only_nan.shape[0]))
            df_prep = df_nan_free

        return df_prep

    @staticmethod
    def prep_training(
        df,
        columns_to_keep=[],
        drop_nan=False,
        categ_columns=[],
        label_column="forecast_status",
        no_delivery_churned_weeks=4,
    ):
        """
        Main preproc method before model training
        :columns_to_keep: List of features columns that should be returned
        :drop_nan: Drop NaN rows
        :categ_columns: List of categorical columns
        :label_column: Label feature (returned as df_y)
        :no_delivery_churned_weeks: Removed customers for training that already churned
        :return: tuple (df_x, df_y)
        """
        print("Dataset size before preprocessing: " + str(df.shape[0]))
        df_prep = df.copy()

        df_prep.loc[
            (df_prep.forecast_status == "freezed"), "forecast_status"
        ] = "active"
        df_prep.loc[(df_prep.forecast_status == "active"), "forecast_status"] = 0
        df_prep.loc[(df_prep.forecast_status == "churned"), "forecast_status"] = 1

        df_prep.forecast_status = df_prep.forecast_status.astype(int)

        df_prep = Prep().prep(
            df_prep,
            columns_to_keep=columns_to_keep,
            drop_nan=drop_nan,
            categ_columns=categ_columns,
            no_delivery_churned_weeks=no_delivery_churned_weeks,
        )

        # Split features & labels
        df_y = df_prep.forecast_status.astype(int)
        df_x = df_prep.loc[:, df_prep.columns != label_column]

        print("Dataset size after preprocessing: " + str(df_x.shape))

        return df_x, df_y
