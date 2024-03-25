import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.azure.storage import BlobConnector
from lmkgroup_ds_utils.constants import Company

from customer_churn.data.preprocess import Preprocessor
from customer_churn.paths import INTERIM_DATA_DIR

logger = logging.getLogger(__name__)


def read_files(
    company_id: str,
    start_date: datetime,
    end_date: datetime,
    local: bool,
    blob_connector: BlobConnector | None = None,
) -> pd.DataFrame:
    if blob_connector is None:
        blob_connector = BlobConnector(local=True)

    logger.info(f"Read files for {company_id} from {start_date} to {end_date}")
    if local:
        p = Path(INTERIM_DATA_DIR).glob("*")
        last_snapshot = sorted([x for x in p if x.is_file()], reverse=True)[0]
        logger.info(f"Reading {last_snapshot}")
        return pd.read_csv(last_snapshot)

    filespath = f"churn-ai/{Company.get_name_from_id(company_id)}/data/interm/"
    last_snapshot = blob_connector.list_blobs("data-science", filespath)[0]
    logger.info(f"Downloading {last_snapshot} from Azure data lake")
    df = blob_connector.download_csv_to_df(last_snapshot)

    return df


def get_features(
    company_id: str,
    start_date: datetime,
    end_date: datetime,
    local: bool,
) -> pd.DataFrame:
    logger.info(f"Get features for {company_id} from {start_date} to {end_date}")

    # if local, read from local file, else download from Databricks mlflow
    df = read_files(company_id, start_date, end_date, local)

    # build features
    df_features = Preprocessor().prep_prediction(df=df)

    return df_features
