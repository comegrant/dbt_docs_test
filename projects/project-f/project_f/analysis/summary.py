# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false
# pyright: reportAssignmentType=false
# pyright: reportCallIssue=false

import pandas as pd


def calculate_total_orders(
    df_non_established: pd.DataFrame,
    df_newly_established: pd.DataFrame,
    df_established: pd.DataFrame,
) -> pd.DataFrame:
    established_no_code_idx = df_non_established[
        df_non_established["customer_segment"] == "Reactivated Customers No Code"
    ].index

    # Merge reactivated customers no code with reactivated customers
    df_non_established.loc[established_no_code_idx, "customer_segment"] = "Reactivated Customers"
    df_non_established = pd.DataFrame(
        df_non_established.groupby(["company_name", "delivery_year", "delivery_week", "customer_segment"])[
            "number_of_orders"
        ].sum()
    ).reset_index()

    df_newly_established = df_newly_established[
        [
            "company_name",
            "delivery_year",
            "delivery_week",
            "number_of_orders",
            "customer_segment",
        ]
    ]

    columns = ["company_name", "delivery_year", "delivery_week", "number_of_orders"]
    df_established = df_established[columns]
    df_established["customer_segment"] = "Established Customers"
    df_established_combined = pd.concat([df_newly_established, df_established])
    df_final = pd.concat([df_non_established, df_established_combined])
    return df_final


def process_orders(df_summary: pd.DataFrame, budget_start: int, budget_end: int) -> pd.DataFrame:
    df_summary = df_summary[
        (df_summary["delivery_year"] * 100 + df_summary["delivery_week"] >= budget_start)
        & (df_summary["delivery_year"] * 100 + df_summary["delivery_week"] <= budget_end)
    ]
    df_summary = df_summary.sort_values(by=["company_name", "delivery_year", "delivery_week", "customer_segment"])

    df_summary_total = (
        pd.DataFrame(df_summary.groupby(["company_name", "delivery_year", "delivery_week"])["number_of_orders"].sum())
        .reset_index()
        .rename(columns={"number_of_orders": "total_orders_all_segments"})
    )
    df_summary = df_summary.merge(df_summary_total)
    return df_summary


def merge_order_with_forecast(df_summary: pd.DataFrame, df_orders_forecast: pd.DataFrame) -> pd.DataFrame:
    df_summary["segment_share"] = df_summary["number_of_orders"] / df_summary["total_orders_all_segments"]
    df_summary = df_summary.merge(df_orders_forecast, how="left")
    return df_summary


def make_seasonal_effect_df(df_summary: pd.DataFrame) -> pd.DataFrame:
    df_seasonal_effect = df_summary[["company_name", "delivery_year", "delivery_week"]].drop_duplicates()
    # default value
    df_seasonal_effect["effect"] = 1.0

    return df_seasonal_effect
