import logging
from datetime import datetime

import pandas as pd
from constants.companies import Company
from data_connector.databricks_connector import is_running_databricks, read_data

from customer_churn.paths import DATA_PROCESSED_DIR

logger = logging.getLogger(__name__)


def read_files(
    company: Company,
    start_date: datetime,
    end_date: datetime,
    env: str,
    schema: str = "mltesting",
) -> pd.DataFrame:
    if is_running_databricks():
        sql_query = (
            f"SELECT * FROM {env}.{schema}.customer_churn_snapshot"
            + f" WHERE snapshot_date BETWEEN '{start_date}' AND '{end_date}'"
            + f" AND company_id = '{company.company_id}'"
        )

        logger.info(sql_query)
        df = read_data(sql_query)
    else:
        df = pd.read_csv(
            DATA_PROCESSED_DIR / company.company_code / "full_snapshot.csv",
        )
    return df


def get_features(
    company: Company,
    start_date: datetime,
    end_date: datetime,
    env: str,
) -> pd.DataFrame:
    logger.info(
        f"Get features for {company.company_name} from {start_date} to {end_date}",
    )

    # if local, read from local file, else download from Databricks mlflow
    df = read_files(company, start_date, end_date, env)

    if df.empty:
        raise ValueError("Empty features!")

    return df


def get_training_data_databricks(
    company: Company,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    env: str = "dev",
    schema: str = "mltesting",
) -> pd.DataFrame:
    if end_date is None:
        sql_query_end_date = (
            "SELECT MAX(snapshot_date) as max_snapshot_date"
            + f" FROM {env}.{schema}.customer_churn_snapshot"
        )
        end_date = pd.to_datetime(
            read_data(sql_query_end_date.format(env=env, schema=schema))[
                "max_snapshot_date"
            ].iloc[0],
        )

    if start_date is None:
        start_date = end_date - pd.Timedelta(days=30)

    logger.info(
        f"Fetching data for end date to {end_date} and start date to {start_date}",
    )

    sql_query = (
        "SELECT * FROM {env}.{schema}.customer_churn_snapshot"
        + " WHERE snapshot_date BETWEEN '{start_date}' AND '{end_date}'"
        + " AND company_id = '{company_id}'"
    )
    df = read_data(
        sql_query.format(
            env=env,
            schema=schema,
            start_date=start_date,
            end_date=end_date,
            company_id=company.company_id,
        ),
    )
    return df


def load_local_training_data(company: Company) -> pd.DataFrame:
    df = pd.read_csv(DATA_PROCESSED_DIR / company.company_code / "full_snapshot.csv")
    return df


def load_training_data(
    company: Company,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    env: str = "dev",
    schema: str = "mltesting",
) -> pd.DataFrame:
    logger.info(
        f"Load training data for {company.company_name} from {start_date} to {end_date}",
    )

    if is_running_databricks():
        return get_training_data_databricks(
            company=company,
            start_date=start_date,
            end_date=end_date,
            env=env,
            schema=schema,
        )

    return load_local_training_data(company)
