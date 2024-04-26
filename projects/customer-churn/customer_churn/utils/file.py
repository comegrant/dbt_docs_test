import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def save_predictions_locally(
    predictions: pd.DataFrame,
    local_file_dir: Path,
    local_filename: str,
) -> None:
    logger.info(f"Saving predictions to path: {local_file_dir}")
    if not local_file_dir.exists():
        logger.info(f"Creating folder {local_file_dir} for predictions")
        local_file_dir.mkdir(parents=True, exist_ok=True)

    prediction_local_full_path = f"{local_file_dir}/{local_filename}"
    predictions.to_csv(prediction_local_full_path, index=False)
