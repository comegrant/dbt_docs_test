import logging

import pandas as pd
from attribute_scoring.common import Args
from attribute_scoring.predict.config import PredictionConfig
from attribute_scoring.predict.data import get_data
from attribute_scoring.predict.utils import filter_data, postprocess_predictions, save_df_to_db
from constants.companies import get_company_by_code
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient

CONFIG = PredictionConfig()


def predict_pipeline(
    args: Args,
    fe: FeatureEngineeringClient,
    spark: DatabricksSession,
    start_yyyyww: int | None = None,
    end_yyyyww: int | None = None,
) -> None:
    """Executes the prediction pipeline for the specified company.

    This function performs batch scoring using the pre-trained model, postprocesses the predictions,
    and saves the results to a Databricks table.

    Args:
        args (Args): Configuration arguments.
        fe (FeatureEngineeringClient): Feature engineering client.
        spark (DatabricksSession): A Spark session.
    """
    company_id = get_company_by_code(args.company).company_id
    data, start_date, end_date = get_data(
        env=args.env, spark=spark, company_id=company_id, start_yyyyww=start_yyyyww, end_yyyyww=end_yyyyww
    )

    logging.info("Preprocessing data.")
    filtered_data = filter_data(env=args.env, spark=spark, company_id=company_id, data=data)

    base_df = None
    for target in CONFIG.target_mapped:
        logging.info(f"Starting prediction for {args.company} ({target})")

        model_uri = CONFIG.model_uri(args.env, args.company, target)
        probability = fe.score_batch(model_uri=model_uri, df=filtered_data, result_type="string").toPandas()

        df = postprocess_predictions(probability, target)

        if base_df is None:
            base_df = pd.DataFrame({"company_id": company_id}, index=df.index)
            base_df = pd.concat([base_df, df.iloc[:, :-2]], axis=1)

        pred_columns = df.iloc[:, -2:]
        base_df = pd.concat([base_df, pred_columns], axis=1)

        logging.info(f"Predictions finished for {args.company} ({target}).")

    save_df_to_db(args=args, spark=spark, df=base_df, start_yyyyww=start_date, end_yyyyww=end_date)

    logging.info("Predictions pipeline completed.")
