import json
import logging

import pandas as pd
from lmkgroup_ds_utils.azure.storage import BlobConnector

from preselector.utils.constants import ENVIRONMENT
from preselector.utils.paths import OUTPUT_DIR

logger = logging.getLogger(__name__)


def upload_file_to_datalake(
    company: str,
    df: pd.DataFrame,
    datalake_handler: BlobConnector,
    file_name: str,
    subdirectory: str,
) -> None:
    remote_filepath = (
        f"personalization/preselector/{ENVIRONMENT}/results/{company}/{subdirectory}"
    )
    datalake_handler.upload_df(
        container="data-science",
        dataframe=df,
        file_path=remote_filepath,
        filename=file_name,
    )


def write_result_to_file(output: list, fn: str):
    output_file = OUTPUT_DIR / fn

    with open(output_file, "w+") as f:
        f.write(json.dumps(output, indent=4))


def save_outputs_locally(output_dict: dict, file_name: str):
    data = pd.DataFrame(output_dict)
    output_file = OUTPUT_DIR / file_name
    data.to_csv(output_file, index=False)
    logger.info(f" {file_name} saved to {output_file}.")


def upload_outputs_to_datalake(
    company: str,
    datalake_handler: BlobConnector,
    df: pd.DataFrame,
    file_name: str,
    subdirectory: str,
):
    upload_file_to_datalake(
        company=company,
        df=df,
        datalake_handler=datalake_handler,
        file_name=file_name,
        subdirectory=subdirectory,
    )
