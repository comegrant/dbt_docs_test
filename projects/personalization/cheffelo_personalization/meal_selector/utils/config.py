# Combines necessary info into a configuration script
# TODO: Create yaml/json file that this config import (one prod, one test, etc)

import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# Creating .env variables
load_dotenv(find_dotenv())
# Build paths inside the project like this: ROOT_DIR / 'subdir'.
ROOT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT_DIR / "resources"


def get_data_dir():
    company = os.getenv("COMPANY")
    if company:
        return DATA_DIR / company
    return DATA_DIR


DATA_DIR = get_data_dir()
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
CONFIG_DIR = ROOT_DIR / "config"


DB = {
    "analytics_db": {
        "dbname": os.getenv("ANALYTICS_DB"),
        "user": os.getenv("ANALYTICS_USR"),
        "host": os.getenv("ANALYTICS_HOST"),
        "port": os.getenv("ANALYTICS_PORT"),
        "passw": os.getenv("ANALYTICS_PWD"),
        "driver": os.getenv("ANALYTICS_DRIVER"),
        "connection_string": os.getenv("ANALYTICS_DB_CONN_STR"),
    },
    "ml_db": {
        "dbname": os.getenv("ML_DB"),
        "user": os.getenv("ML_USR"),
        "host": os.getenv("ML_HOST"),
        "port": os.getenv("ML_PORT"),
        "passw": os.getenv("ML_PWD"),
        "driver": os.getenv("ML_DRIVER"),
        "connection_string": os.getenv("ML_DB_CONN_STR"),
    },
}
