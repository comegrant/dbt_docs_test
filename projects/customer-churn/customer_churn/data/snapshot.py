# Holds the dataprep logic.

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.constants import Company

from customer_churn.data.load_data import DataLoader

logger = logging.getLogger(__name__)


def generate_snapshots_for_period(
    data_loader: DataLoader,
    company_id: str,
    start_date: datetime,
    end_date: datetime,
    save_snapshot: bool = False,
    output_dir: str = "",
    output_file_prefix: str = "snapshot_",
) -> list:
    snapshot_dates = get_snapshot_dates_based_on_company(
        start_date,
        end_date,
        company_id,
        save_snapshot,
        output_dir,
        output_file_prefix,
    )
    for snapshot_date in snapshot_dates:
        generate_snapshot_for_date(data_loader, snapshot_date)


def generate_snapshot_for_date(
    data_loader: DataLoader,
    snapshot_date: str,
    save_snapshot: bool = False,
    output_dir: str = "",
    output_file_prefix: str = "snapshot_",
) -> None:
    snapshot_dt_start = time.time()

    # Get customers for given snapshot date
    logger.info(
        f"Getting snapshot for: {snapshot_date}",
    )

    out_file = output_dir + output_file_prefix + snapshot_date.replace("-", "") + ".csv"

    logger.info(f"Output file: {out_file}")

    # Generate data for the given date
    df_out = data_loader.generate_features_for_date(snapshot_date=snapshot_date)

    with Path.open(out_file, "a") as f:
        df_out.to_csv(out_file, mode="a", header=f.tell() == 0, index=False)

    snapshot_dt_end = time.time()
    logger.info(
        "Snapshot done in " + str(int(snapshot_dt_end - snapshot_dt_start)) + " (sec)",
    )

    if save_snapshot is not None:
        save_snapshot(out_file)


def get_snapshot_dates_based_on_company(
    snapshot_start_date: str,
    snapshot_end_date: str,
    company_id: str,
) -> list:
    # Get snapshot dates based on company orders cutoff date
    if company_id == Company.RN:
        return (
            pd.date_range(
                start=snapshot_start_date,
                end=snapshot_end_date,
                freq="W-FRI",  # RN orders cutoff Thursday
            )
            .strftime("%Y-%m-%d")
            .tolist()
        )
    else:
        return (
            pd.date_range(
                start=snapshot_start_date,
                end=snapshot_end_date,
                freq="W-WED",  # All others orders cutoff Tuesday
            )
            .strftime("%Y-%m-%d")
            .tolist()
        )
