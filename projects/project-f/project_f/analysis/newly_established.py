# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportCallIssue=false

import numpy as np
import pandas as pd
from project_f.analysis.non_established import calculate_orders


def get_newly_established_cohorts(
    df_cohort_latest: pd.DataFrame,
    df_weekly_decay_rate: pd.DataFrame,
    max_established_cohort_week: int,
) -> pd.DataFrame:
    # non established
    max_ne_cohort_week = df_cohort_latest["cohort_week"].max()  # should be 8
    df_cohort_end = df_cohort_latest[df_cohort_latest["cohort_week"] == max_ne_cohort_week]
    df_cohort_end = df_cohort_end.merge(df_weekly_decay_rate)

    df_cohort_newly_estaliblished_ls = []
    for cohort_week in np.arange((max_ne_cohort_week + 1), max_established_cohort_week):
        df_cohort_newly_estaliblished = df_cohort_end.copy()
        df_cohort_newly_estaliblished.loc[:, "cohort_week"] = int(cohort_week)
        df_cohort_newly_estaliblished["average_orders_compouneded"] = df_cohort_newly_estaliblished[
            "average_orders"
        ] * (df_cohort_newly_estaliblished["decay_rate"] ** (cohort_week - 8))

        df_cohort_newly_estaliblished_ls.append(df_cohort_newly_estaliblished)

    df_cohort_newly_estaliblished = pd.concat(df_cohort_newly_estaliblished_ls)
    df_cohort_newly_estaliblished = df_cohort_newly_estaliblished[
        [
            "company_name",
            "year",
            "month",
            "customer_segment",
            "cohort_week",
            "average_orders_compouneded",
        ]
    ].rename(columns={"average_orders_compouneded": "average_orders"})

    return df_cohort_newly_estaliblished


def calculate_orders_newly_established(
    df_cohort_newly_estaliblished: pd.DataFrame,
    df_started_delivery: pd.DataFrame,
    df_source_effect: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _, df_orders_newly_established = calculate_orders(
        df_cohort_latest=df_cohort_newly_estaliblished,
        df_started_delivery=df_started_delivery,
        df_source_effect=df_source_effect,
    )

    df_orders_newly_established["customer_segment"] = (
        "Established From " + df_orders_newly_established["customer_segment"]
    )

    df_orders_newly_established_agg = pd.DataFrame(
        df_orders_newly_established.groupby(["company_name", "customer_segment", "delivery_year", "delivery_week"])[
            "number_of_orders"
        ].sum()
    ).reset_index()
    return df_orders_newly_established, df_orders_newly_established_agg
