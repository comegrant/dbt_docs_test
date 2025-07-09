# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false
# pyright: reportAssignmentType=false

from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st
from catalog_connector import connection
from dotenv import load_dotenv
from project_f.paths import DATA_DIR, DATA_DOWNLOADED_DIR, SQL_DIR


def section_download_data(budget_start: int, budget_year: int, budget_type: str) -> None:
    # Load environment variables from the .env file
    load_dotenv()

    if not DATA_DIR.exists():
        st.write(f"Creating a local directory {DATA_DIR}")
        Path.mkdir(DATA_DIR)
    if not DATA_DOWNLOADED_DIR.exists():
        st.write(f"Creating a local directory {DATA_DOWNLOADED_DIR}")
        Path.mkdir(DATA_DOWNLOADED_DIR)

    sql_names = [
        "budget_calendar",
        # "marketing_input",
        "new_customers",
        "reactivated_customers",
        "established_customers",
        "orders_forecast",
        "previous_budget",
        "total_orders_per_week",
    ]
    num_sqls = len(sql_names)
    budget_start_year = budget_start // 100
    min_year = budget_start_year - 1
    for idx, sql_name in enumerate(sql_names):
        st.write(f"{idx + 1}/{num_sqls}, {sql_name}...")
        if sql_name == "previous_budget":
            df = fetch_data_from_sql(sql_file_name=sql_name, directory=SQL_DIR, min_year=min_year)
        elif sql_name == "marketing_input":
            df = fetch_data_from_sql(
                sql_file_name=sql_name, directory=SQL_DIR, budget_year=budget_year, budget_type=budget_type
            )
        else:
            df = fetch_data_from_sql(sql_file_name=sql_name, directory=SQL_DIR)
        df.to_csv(DATA_DOWNLOADED_DIR / f"{sql_name}.csv", index=False)

    st.write("Done!")


def fetch_data_from_sql(
    sql_file_name: str,
    directory: Path,
    **kwargs: Optional[dict[str, Any]],
) -> pd.DataFrame:
    """Fetch data from sql

    Args:
        sql_name (str): sql name, without extension
        directory (Optional[Path], optional): directory of the sql file.
        Defaults to SQL_DIR.

    Returns:
        pd.DataFrame
    """

    with Path.open(directory / f"{sql_file_name}.sql") as f:
        query = f.read().format(**kwargs)

    df = connection.sql(query).toPandas()

    return df
