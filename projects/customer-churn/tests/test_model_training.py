import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from customer_churn.model_logreg import ModelLogReg
from customer_churn.paths import DATA_DIR

logger = logging.getLogger(__name__)

FEATURES = [
    "agreement_id",
    "snapshot_date",
    "year",
    "month",
    "week",
    "number_of_total_orders",
    "weeks_since_last_delivery",
    "customer_since_weeks",
    "total_complaints",
    "category",
    "weeks_since_last_complaint",
    "number_of_complaints_last_N_weeks",
    "total_normal_activities",
    "total_account_activities",
    "number_of_normal_activities_last_N_weeks",
    "number_of_account_activities_last_N_weeks",
    "average_normal_activity_difference",
    "average_account_activity_difference",
    "snapshot_status",
    "planned_delivery",
]

PREP_CONFIG = {
    "snapshot": {
        "start_date": "2023-01-10",  # Data snapshot start date
        "end_date": datetime.now(tz="UTC").strftime(
            "%Y-%m-%d",
        ),  # Data snapshot end date
        "forecast_weeks": 4,  # Prediction n-weeks ahead
        "buy_history_churn_weeks": 4,  # Number of customer weeks to include in data model history
    },
    "output_dir": "/processed/",  # Output directory
}


@pytest.mark.parametrize(
    "input_data",
    Path("tests/training") / "snapshot_training_sample.csv",
)
def test_model_training(input_data: Path) -> None:
    logger.info(f"Reading {input_data} data from file...")
    with Path.open(DATA_DIR / input_data) as f:
        df = pd.read_csv(f)

    model = ModelLogReg(data=df, config=PREP_CONFIG, probability_threshold=0.3)
    model.prep_training()
    model.fit()
