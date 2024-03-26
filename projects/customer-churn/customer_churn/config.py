# Combines necessary info into a configuration script

import os

from dotenv import find_dotenv, load_dotenv

from customer_churn.constants import CompanyID

# Creating .env variables
load_dotenv(find_dotenv())


DB = {
    "read_db": {
        "dbname": os.getenv("READ_DB"),
        "user": os.getenv("READ_USR"),
        "host": os.getenv("READ_HOST"),
        "port": os.getenv("READ_PORT"),
        "passw": os.getenv("READ_PWD"),
        "driver": os.getenv("READ_DRIVER"),
    },
    "write_db": {
        "dbname": os.getenv("WRITE_DB"),
        "user": os.getenv("WRITE_USR"),
        "host": os.getenv("WRITE_HOST"),
        "port": os.getenv("WRITE_PORT"),
        "passw": os.getenv("WRITE_PWD"),
        "driver": os.getenv("WRITE_DRIVER"),
    },
}

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
        "customers": "../data/raw/customers.csv",
        "events": "../data/raw/events.csv",
        "orders": "../data/raw/orders.csv",
        "crm_segments": "../data/raw/crm_segments_cleared.csv",
        "complaints": "../data/raw/complaints.csv",
        "bisnode": "../data/raw/bisnode_enrichments.csv",
    },
    "output_dir": "../data/processed/",  # Output directory
    "company_id": CompanyID.RN,  # Company ID
}
