import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from customer_churn.features import Features
from lmkgroup_ds_utils.constants import Company

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    (
        "expected_results",
        "model_training",
        "snapshot_date",
        "agreement_id",
        "prep_config",
    ),
    [
        (
            {
                "snapshot_status": "active",
                "forecast_status": "freezed",
                "week": 41,
                "month": 10,
                "year": 2020,
                "customer_since_weeks": 25,
                "weeks_since_last_delivery": 3,
                "confidence_level": "green",
                "children_probability": 30,
                "weeks_since_last_complaint": -1,
            },
            True,
            pd.to_datetime("2020-10-10"),
            112211,
            {
                "snapshot": {
                    "start_date": "2021-01-01",  # Data snapshot start date
                    "end_date": "2021-11-01",  # Data snapshot end date
                    "forecast_weeks": 4,  # Prediction n-weeks ahead
                    "buy_history_churn_weeks": 4,  # Number of customer weeks to include in data model history
                    "complaints_last_n_weeks": 4,
                    "onboarding_weeks": 12,
                },
                "input_files": {
                    "customers": Path("tests/snapshot_test") / "customers.csv",
                    "events": Path("tests/snapshot_test") / "events.csv",
                    "orders": Path("tests/snapshot_test") / "orders.csv",
                    "crm_segments": Path("tests/snapshot_test") / "crm_segments_cleared.csv",
                    "complaints": Path("tests/snapshot_test") / "complaints.csv",
                    "bisnode": Path("tests/snapshot_test") / "bisnode_enrichments.csv",
                },
                "output_dir": Path("tests/snapshot_test") / "processed",  # Output directory
                "company_id": Company.RN,  # Company ID
            },
        ),
        (
            {
                "agreement_id": 112211,
                "snapshot_status": "active",
                "forecast_status": "churned",
                "week": 43,
                "month": 10,
                "year": 2021,
                "customer_since_weeks": 51,
                "weeks_since_last_delivery": 1,
                "confidence_level": "green",
                "children_probability": 0,
                "weeks_since_last_complaint": 43,
            },
            True,
            pd.to_datetime("2021-10-25"),
            112211,
            {
                "snapshot": {
                    "start_date": "2021-01-01",  # Data snapshot start date
                    "end_date": "2021-11-01",  # Data snapshot end date
                    "forecast_weeks": 4,  # Prediction n-weeks ahead
                    "buy_history_churn_weeks": 4,  # Number of customer weeks to include in data model history
                    "complaints_last_n_weeks": 4,
                    "onboarding_weeks": 12,
                },
                "input_files": {
                    "customers": Path("tests/snapshot_test2") / "customers.csv",
                    "events": Path("tests/snapshot_test2") / "events.csv",
                    "orders": Path("tests/snapshot_test2") / "orders.csv",
                    "crm_segments": Path("tests/snapshot_test2") / "crm_segments_cleared.csv",
                    "complaints": Path("tests/snapshot_test2") / "complaints.csv",
                    "bisnode": Path("tests/snapshot_test2") / "bisnode_enrichments.csv",
                },
                "output_dir": Path("tests/snapshot_test2") / "processed",  # Output directory
                "company_id": Company.RN,  # Company ID
            },
        ),
    ],
)
def test_get_snapshot_features_for_date_single_agreement(
    expected_results: dict,
    model_training: bool,
    snapshot_date: datetime,
    agreement_id: int,
    prep_config: dict,
) -> None:
    # forecast_status = freezed (changed during prediction period)
    features = Features(
        company_id=Company.RN,
        prep_config=prep_config,
        model_training=model_training,
    )

    snapshot_features = features.get_snapshot_features_for_date(
        snapshot_date=snapshot_date,
    )

    customer_features = snapshot_features[snapshot_features.agreement_id == agreement_id].iloc[0]

    logger.info(customer_features)

    for col in expected_results:
        assert (
            customer_features[col] == expected_results[col]
        ), f"Checking {col}: {expected_results[col]} == {customer_features[col]}"
