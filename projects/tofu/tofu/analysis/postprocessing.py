# pyright: reportAttributeAccessIssue=false
# pyright: reportCallIssue=false
# pyright: reportReturnType=false
# pyright: reportArgumentType=false

import uuid
from datetime import datetime

import pandas as pd


def calculate_forecasts(
    df_future_mapped: pd.DataFrame,
    df_future_mapped_with_flex: pd.DataFrame,
) -> pd.DataFrame:
    df_forecast = df_future_mapped[
        [
            "menu_year",
            "menu_week",
            "analysts_forecast_total_orders",
        ]
    ].merge(
        df_future_mapped_with_flex[
            [
                "menu_year",
                "menu_week",
                "analysts_forecast_flex_share",
            ]
        ],
        on=["menu_year", "menu_week"],
        how="left",
    )

    df_forecast["analysts_forecast_flex_orders"] = (
        df_forecast["analysts_forecast_flex_share"] * df_forecast["analysts_forecast_total_orders"]
    ).astype(int)
    df_forecast["analysts_forecast_non_flex_orders"] = (
        df_forecast["analysts_forecast_total_orders"] - df_forecast["analysts_forecast_flex_orders"]
    )

    return df_forecast


def prepare_run_metadata(
    df_forecast: pd.DataFrame,
    company_id: str,
    forecast_start_year: int,
    forecast_start_week: int,
    forecast_model_id: str,
    forecast_group_ids: dict,
    forecast_job_id: str,
    forecast_job_run_id: str,
    forecast_horizon: int,
    created_at: datetime,
) -> dict:
    df_run_metadata_1 = df_forecast[["menu_year", "menu_week"]].drop_duplicates()
    df_run_metadata_2 = df_forecast[["menu_year", "menu_week"]].drop_duplicates()
    df_run_metadata_1["forecast_group_id"] = forecast_group_ids["non_flex"]
    df_run_metadata_2["forecast_group_id"] = forecast_group_ids["flex"]

    df_run_metadata = pd.concat([df_run_metadata_1, df_run_metadata_2], ignore_index=True)
    df_run_metadata["forecast_job_run_id"] = forecast_job_run_id
    df_run_metadata["forecast_job_run_parent_id"] = ""
    df_run_metadata["forecast_job_id"] = forecast_job_id
    df_run_metadata["forecast_model_id"] = forecast_model_id
    df_run_metadata["company_id"] = company_id
    df_run_metadata["next_cutoff_menu_week"] = forecast_start_year * 100 + forecast_start_week
    df_run_metadata["forecast_horizon"] = forecast_horizon
    df_run_metadata["created_at"] = created_at

    df_run_metadata["forecast_job_run_metadata_id"] = [str(uuid.uuid4()).upper() for _ in range(len(df_run_metadata))]

    col_sequence = [
        "forecast_job_run_metadata_id",
        "forecast_job_run_id",
        "forecast_job_run_parent_id",
        "forecast_job_id",
        "company_id",
        "forecast_group_id",
        "forecast_model_id",
        "menu_year",
        "menu_week",
        "next_cutoff_menu_week",
        "forecast_horizon",
        "created_at",
    ]
    df_run_metadata = df_run_metadata[col_sequence]
    df_run_metadata.sort_values(by=["menu_year", "menu_week", "forecast_group_id"])
    int_columns = ["menu_year", "menu_week", "next_cutoff_menu_week", "forecast_horizon"]
    df_run_metadata[int_columns] = df_run_metadata[int_columns].astype("Int32")
    return df_run_metadata


def prepare_job_run_data(
    df_run_metadata: pd.DataFrame,
) -> pd.DataFrame:
    df_forecast_job_runs = df_run_metadata.copy()
    df_forecast_job_runs["forecast_weeks"] = (
        df_forecast_job_runs["menu_year"] * 100 + df_forecast_job_runs["menu_week"]
    ).astype("Int32")
    df_forecast_job_runs = pd.DataFrame(
        df_forecast_job_runs.groupby(["forecast_job_run_id", "forecast_job_id", "forecast_horizon", "created_at"])[
            "forecast_weeks"
        ].apply(lambda x: list(set(x)))
    ).reset_index()
    col_sequence = ["forecast_job_run_id", "forecast_job_id", "forecast_weeks", "forecast_horizon", "created_at"]

    df_forecast_job_runs = df_forecast_job_runs[col_sequence]
    df_forecast_job_runs["forecast_horizon"] = df_forecast_job_runs["forecast_horizon"].astype("Int32")
    return df_forecast_job_runs


def prepare_forecast_orders_data(
    df_forecast: pd.DataFrame,
    df_run_metadata: pd.DataFrame,
    forecast_group_ids: dict,
) -> pd.DataFrame:
    df_forecast_non_flex = df_forecast[["menu_year", "menu_week", "analysts_forecast_non_flex_orders"]].rename(
        columns={"analysts_forecast_non_flex_orders": "forecast_order_quantity"}
    )

    df_forecast_non_flex["forecast_group_id"] = forecast_group_ids["non_flex"]
    df_forecast_flex = df_forecast[["menu_year", "menu_week", "analysts_forecast_flex_orders"]].rename(
        columns={"analysts_forecast_flex_orders": "forecast_order_quantity"}
    )
    df_forecast_flex["forecast_group_id"] = forecast_group_ids["flex"]

    df_forecast_orders = pd.concat([df_forecast_non_flex, df_forecast_flex], ignore_index=True)

    df_forecast_orders["forecast_orders_id"] = [str(uuid.uuid4()).upper() for _ in range(len(df_forecast_orders))]

    df_forecast_orders = df_run_metadata[
        [
            "forecast_job_run_metadata_id",
            "menu_year",
            "menu_week",
            "company_id",
            "forecast_group_id",
            "forecast_model_id",
            "created_at",
        ]
    ].merge(df_forecast_orders, on=["menu_year", "menu_week", "forecast_group_id"], how="left")

    df_forecast_orders["forecast_orders_id"] = [str(uuid.uuid4()).upper() for _ in range(len(df_forecast_orders))]

    col_sequence = [
        "forecast_orders_id",
        "forecast_job_run_metadata_id",
        "menu_year",
        "menu_week",
        "company_id",
        "forecast_group_id",
        "forecast_model_id",
        "forecast_order_quantity",
        "created_at",
    ]

    df_forecast_orders = df_forecast_orders[col_sequence]
    int_columns = ["menu_year", "menu_week", "forecast_order_quantity"]
    df_forecast_orders[int_columns] = df_forecast_orders[int_columns].astype("Int32")
    df_forecast_orders = (
        df_forecast_orders.sort_values(by=["menu_year", "menu_week", "forecast_group_id"])
        .reset_index()
        .drop(columns=["index"])
    )
    return df_forecast_orders


def calculate_forecast_adjustments(
    df_forecast: pd.DataFrame,
    df_latest_forecasts: pd.DataFrame,
) -> pd.DataFrame:
    df_diff = df_forecast[
        [
            "menu_year",
            "menu_week",
            "analysts_forecast_total_orders",
        ]
    ].merge(
        df_latest_forecasts[["menu_year", "menu_week", "forecast_total_orders"]],
        on=["menu_year", "menu_week"],
        how="inner",
    )

    df_diff["difference"] = df_diff["analysts_forecast_total_orders"] - df_diff["forecast_total_orders"]
    return df_diff
