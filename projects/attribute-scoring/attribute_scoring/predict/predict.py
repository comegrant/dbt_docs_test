import datetime as dt
import logging
import uuid

import pandas as pd
import pytz
from attribute_scoring.common import Args
from attribute_scoring.predict.configs import PredictionConfig
from attribute_scoring.predict.data import get_prediction_data
from attribute_scoring.predict.utils import (
    log_removed_recipes,
    postprocess_predictions,
    save_outputs_to_databricks,
    update_is_latest_flag,
)
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
    data, start_date, end_date = get_prediction_data(
        spark=spark, company_id=company_id, start_yyyyww=start_yyyyww, end_yyyyww=end_yyyyww
    )

    logging.info("Preprocessing data.")
    log_removed_recipes(spark=spark, company_id=company_id, start_yyyyww=start_date, end_yyyyww=end_date)

    base_df = None
    for target in CONFIG.target_mapped:
        logging.info(f"Starting prediction for {args.company} ({target})")

        target_name = CONFIG.target_mapped.get(target)
        if target_name is None:
            raise ValueError(f"No mapping found for target {target}")

        model_uri = CONFIG.model_uri(args.env, args.company, target_name)
        probability = fe.score_batch(model_uri=model_uri, df=data, result_type="string").toPandas()

        df = postprocess_predictions(probability, target)

        if base_df is None:
            base_df = pd.DataFrame({"company_id": company_id}, index=df.index)
            base_df = pd.concat([base_df, df.iloc[:, :-2]], axis=1)

        pred_columns = df.iloc[:, -2:]
        base_df = pd.concat([base_df, pred_columns], axis=1)

        logging.info(f"Predictions finished for {args.company} ({target}).")

    if base_df is None:
        raise ValueError("No predictions were generated - base_df is None")

    update_is_latest_flag(spark=spark, table_name=f"{CONFIG.output_schema}.{CONFIG.output_table}", data=base_df)

    company_id = get_company_by_code(args.company).company_id
    created_at = dt.datetime.now(pytz.timezone("cet")).replace(tzinfo=None)
    run_id = str(uuid.uuid4())

    base_df["created_at"] = created_at
    base_df["run_id"] = run_id
    base_df["is_latest"] = True

    save_outputs_to_databricks(
        spark_df=spark.createDataFrame(base_df.astype(CONFIG.output_columns)),  # type: ignore
        table_name=CONFIG.output_table,
        table_schema=CONFIG.output_schema,
    )

    metadata = [(run_id, created_at, company_id, start_date, end_date)]
    metadata_df = spark.createDataFrame(  # type: ignore
        metadata, ["run_id", "run_timestamp", "company_id", "start_yyyyww", "end_yyyyww"]
    )

    save_outputs_to_databricks(
        spark_df=metadata_df, table_name=CONFIG.output_table + "_metadata", table_schema=CONFIG.output_schema
    )

    logging.info("Predictions pipeline completed.")
