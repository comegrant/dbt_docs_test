import random
from datetime import datetime, timedelta, timezone

import pandas as pd


def date_from_week(year: int, month: int, week: int) -> datetime.date:
    """
    Create a date from year, month, and week numbers.

    Parameters:
    - year: int, the year
    - month: int, the month (1-12)
    - week: int, the week number within the month (1st week, 2nd week, etc.)

    Returns:
    - datetime.date object representing the Monday of the specified week
    """
    # First day of the given year and month
    first_day = datetime(year, month, 1, tzinfo=timezone.utc)
    # Day of week (0 is Monday, 6 is Sunday)
    start_day_of_week = first_day.weekday()
    # Calculate the start date of the first week
    # If the first day is not Monday, adjust to the previous Monday
    start_date_of_first_week = (
        first_day - timedelta(days=start_day_of_week)
        if start_day_of_week
        else first_day
    )

    # Calculate the start date of the given week
    week_start_date = start_date_of_first_week + timedelta(weeks=week - 1)
    return week_start_date


def generate_training_data() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "agreement_id": random.randint(0, 1000000),  # 112211,
                "snapshot_status": random.choice(["active", "churned"]),
                "forecast_status": random.choice(["active", "churned"]),
                "week": random.randint(1, 20),
                "month": random.randint(1, 4),
                "year": 2024,
                "day": random.randint(1, 7),
                "customer_since_weeks": random.randint(1, 150),
                "weeks_since_last_delivery": random.randint(1, 10),
                "confidence_level": "green",
                "children_probability": random.randint(1, 80),
                "weeks_since_last_complaint": -1,
                "number_of_total_orders": random.randint(0, 32),
                "total_complaints": random.randint(0, 32),
                "category": "normal",
                "number_of_complaints_last_N_weeks": random.randint(0, 32),
                "total_normal_activities": random.randint(0, 32),
                "total_account_activities": random.randint(0, 32),
                "number_of_normal_activities_last_N_weeks": random.randint(0, 32),
                "number_of_account_activities_last_N_weeks": random.randint(0, 32),
                "average_normal_activity_difference": random.randint(0, 32),
                "average_account_activity_difference": random.randint(0, 32),
                "planned_delivery": random.choice([True, False]),
            }
            for _ in range(100)
        ],
    )

    df["snapshot_date"] = df.apply(
        lambda x: date_from_week(x["year"], x["month"], x["week"]),
        axis=1,
    )

    return df
