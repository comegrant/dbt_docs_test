import logging
from pathlib import Path

import pandas as pd
import pytest
from customer_churn.features import Features
from lmkgroup_ds_utils.constants import Company

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

TEST_DATA_DIR = Path("tests/snapshot_test")
PREP_CONFIG = {
    "snapshot": {
        "start_date": "2021-01-01",  # Data snapshot start date
        "end_date": "2021-11-01",  # Data snapshot end date
        "forecast_weeks": 4,  # Prediction n-weeks ahead
        "buy_history_churn_weeks": 4,  # Number of customer weeks to include in data model history
        "complaints_last_n_weeks": 4,
        "onboarding_weeks": 12,
    },
    "input_files": {
        "customers": TEST_DATA_DIR / "customers.csv",
        "events": TEST_DATA_DIR / "events.csv",
        "orders": TEST_DATA_DIR / "orders.csv",
        "crm_segments": TEST_DATA_DIR / "crm_segments_cleared.csv",
        "complaints": TEST_DATA_DIR / "complaints.csv",
        "bisnode": TEST_DATA_DIR / "bisnode_enrichments.csv",
    },
    "output_dir": TEST_DATA_DIR / "processed",  # Output directory
    "company_id": Company.RN,  # Company ID
}


@pytest.mark.asyncio()
def test_prep_get_snapshot_case_1() -> None:
    # forecast_status = freezed (changed during prediction period)

    snapshot_date = pd.to_datetime("2021-10-25")
    agreement_id = 112211
    features = Features(
        company_id=Company.RN,
        prep_config=PREP_CONFIG,
    )

    snapshot_features = features.get_snapshot_features_for_date(
        snapshot_date=snapshot_date,
    )

    customer_features = snapshot_features[
        snapshot_features.agreement_id == agreement_id
    ].iloc[0]

    assert customer_features
