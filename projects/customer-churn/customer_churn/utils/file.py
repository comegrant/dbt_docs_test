import logging
import pandas as pd
from typing import Optional
from pathlib import Path

from lmkgroup_ds_utils.db.connector import DB
from lmkgroup_ds_utils.azure.storage import BlobConnector

from customer_churn.paths import OUTPUT_DIR

logger = logging.getLogger(__name__)


def save_and_upload_results_to_datalake(
    datalake_handler: BlobConnector,
    predictions: pd.DataFrame,
    filename: str,
    dl_path: Path,
    local_path: Path = OUTPUT_DIR,
):
    save_predictions_locally(
        predictions=predictions,
        local_file_dir=local_path,
        local_filename=filename
    )
    upload_file_to_datalake(
        datalake_handler=datalake_handler,
        local_file_dir=local_path,
        local_filename=filename,
        remote_dir=dl_path,
        remote_filename=filename
    )

def save_predictions_locally(predictions: pd.DataFrame, local_file_dir: Path, local_filename: str):
    logger.info(f"Saving predictions to path: {local_file_dir}")
    if not local_file_dir.exists():
        logger.info(f"Creating folder {local_file_dir} for predictions")
        local_file_dir.mkdir(parents=True, exist_ok=True)

    prediction_local_full_path = f"{local_file_dir}/{local_filename}"
    predictions.to_csv(prediction_local_full_path, index=False)



def upload_file_to_datalake(
    datalake_handler: BlobConnector,
    local_file_dir: Path,
    local_filename: str,
    remote_dir: Path,
    remote_filename: Optional[str] = None,
    container: Optional[str] = "data-science",
) -> None:
    if remote_filename is None:
        remote_filename = local_filename
    local_file_path = local_file_dir / local_filename
    datalake_handler.upload_local_file(
        container=container,
        local_file_path=local_file_path,
        remote_file_path=remote_dir,
        filename=remote_filename)
    logger.info(f"{local_filename} uploaded to {remote_dir}/{remote_filename}")