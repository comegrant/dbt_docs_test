import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def get_features(
    company_id: str,
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    logger.info(f"Get features for {company_id} from {start_date} to {end_date}")
    return pd.DataFrame()
