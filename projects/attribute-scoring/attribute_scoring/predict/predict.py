import datetime as dt
import uuid

import pandas as pd
import pytz
from attribute_scoring.common import ArgsPredict
from attribute_scoring.predict.configs import PredictionConfig

from attribute_scoring.predict.data import (
    get_prediction_data,
    postprocessing,
)
from attribute_scoring.predict.utils import (
    update_is_latest_flag,
    save_outputs_to_databricks,
)
from constants.companies import get_company_by_code
from model_registry import databricks_model_registry, ModelRegistryBuilder

CONFIG = PredictionConfig()


async def predict_pipeline(
    args: ArgsPredict,
    registry: ModelRegistryBuilder = databricks_model_registry(),
) -> None:
    company = get_company_by_code(args.company)

    data, start_date, end_date = get_prediction_data(
        company.company_id,
        args.startyyyyww,
        args.endyyyyww,
    )

    entities = pd.DataFrame(data[["recipe_id", "recipe_portion_id", "language_id"]])

    base_df = None
    for target in CONFIG.target_mapped:
        target_name = CONFIG.target_mapped.get(target)
        if target_name is None:
            raise ValueError(f"No mapping found for target {target}")

        model_uri = CONFIG.model_uri(args.env, args.company, target_name, args.alias)
        output = await registry.infer_over(
            entities, model_uri=model_uri, output_name="probability"
        )

        predictions = postprocessing(output, company.company_id, target_name)

        if base_df is None:
            base_df = predictions
        else:
            base_df = pd.merge(
                base_df, predictions, on=["company_id", "recipe_id"], how="inner"
            )

    if base_df is None:
        raise ValueError("No predictions were generated - base_df is None")

    created_at = dt.datetime.now(pytz.timezone("cet")).replace(tzinfo=None)
    run_id = str(uuid.uuid4())

    base_df["created_at"] = created_at
    base_df["run_id"] = run_id
    base_df["is_latest"] = True

    base_df = base_df.astype(CONFIG.output_columns)

    update_is_latest_flag(table_name="mloutputs.attribute_scoring", data=base_df)
    save_outputs_to_databricks(base_df, table_name="attribute_scoring")

    metadata = [(run_id, created_at, company.company_id, start_date, end_date)]
    metadata_df = pd.DataFrame(metadata, columns=CONFIG.metadata_columns)  # type: ignore

    save_outputs_to_databricks(metadata_df, table_name="attribute_scoring_metadata")
