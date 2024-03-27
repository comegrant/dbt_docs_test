import random

import pandas as pd


def generate_training_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "agreement_id": random.randint(0, 1000000),  # 112211,
                "snapshot_status": random.choice(["active", "churned"]),
                "forecast_status": random.choice(["active", "churned"]),
                "week": random.randint(1, 51),
                "month": random.randint(1, 12),
                "year": random.randint(2021, 2024),
                "customer_since_weeks": random.randint(1, 150),
                "weeks_since_last_delivery": random.randint(1, 10),
                "confidence_level": "green",
                "children_probability": random.randint(1, 80),
                "weeks_since_last_complaint": -1,
            }
            for _ in range(10)
        ],
    )
