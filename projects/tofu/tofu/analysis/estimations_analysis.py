# pyright: reportReturnType=false
# pyright: reportCallIssue=false
# pyright: reportArgumentType=false
import numpy as np
import pandas as pd


def get_latest_estimations(
    df_estimations: pd.DataFrame,
    df_future_mapped: pd.DataFrame,
) -> pd.DataFrame:
    df_latest_estimations = (
        df_estimations.merge(df_future_mapped[["menu_year", "menu_week"]], how="inner")
        .dropna(subset=["total_orders_estimated"])
        .sort_values(by=["menu_year", "menu_week", "estimation_generated_at"], ascending=[True, True, False])
        .drop_duplicates(subset=["menu_year", "menu_week"], keep="first")
    )

    return df_latest_estimations


def get_estimations_pivot_table(
    df_estimations: pd.DataFrame,
    df_future_mapped: pd.DataFrame,
    df_known_mapped: pd.DataFrame,
    num_weeks_known_to_include: int = 5,
) -> tuple[pd.DataFrame, list[int]]:
    weeks_to_include = list(
        (df_known_mapped["menu_year"] * 100 + df_known_mapped["menu_week"]).nlargest(num_weeks_known_to_include)
    ) + list((df_future_mapped["menu_year"] * 100 + df_future_mapped["menu_week"]).unique())

    df_estimations_to_show = df_estimations[
        (df_estimations["menu_year"] * 100 + df_estimations["menu_week"]).isin(weeks_to_include)
    ]

    df_estimations_to_show = df_estimations_to_show.merge(
        df_known_mapped[["menu_year", "menu_week", "total_orders"]],
        how="left",
        on=["menu_year", "menu_week"],
    )

    df_estimations_to_show.loc[df_estimations_to_show["num_days_before_cutoff"] == 0, "total_orders_estimated"] = (
        df_estimations_to_show.loc[df_estimations_to_show["num_days_before_cutoff"] == 0, "total_orders"]
    )

    df_estimations_to_show = df_estimations_to_show.drop_duplicates(
        subset=["menu_year", "menu_week", "num_days_before_cutoff"]
    )
    df_estimations_pivoted = df_estimations_to_show.pivot(
        index="num_days_before_cutoff", columns="menu_week", values="total_orders_estimated"
    )[::-1]
    df_estimations_pivoted = df_estimations_pivoted.astype(int, errors="ignore")

    return df_estimations_pivoted, weeks_to_include


def get_estimations_delta_table(
    df_estimations: pd.DataFrame,
    df_known_mapped: pd.DataFrame,
    df_latest_estimations: pd.DataFrame,
    weeks_to_include: list[int],
) -> pd.DataFrame:
    df_estimations_relevant = df_estimations[
        (df_estimations["menu_week"] + df_estimations["menu_year"] * 100).isin(weeks_to_include)
        & (
            df_estimations["num_days_before_cutoff"].isin(
                list(df_latest_estimations["num_days_before_cutoff"].unique()) + [0]  # noqa
            )
        )
    ]

    df_estimations_relevant = df_estimations_relevant.merge(
        df_known_mapped[["menu_year", "menu_week", "total_orders"]],
        how="left",
        on=["menu_year", "menu_week"],
    ).drop_duplicates(
        subset=["menu_year", "menu_week", "num_days_before_cutoff"],
    )

    df_estimations_relevant.loc[df_estimations_relevant["num_days_before_cutoff"] == 0, "total_orders_estimated"] = (
        df_estimations_relevant.loc[df_estimations_relevant["num_days_before_cutoff"] == 0, "total_orders"]
    )

    num_days_before_cutoff = np.sort(df_estimations_relevant["num_days_before_cutoff"].unique())
    df_mapping = pd.DataFrame(
        {
            "num_days_before_cutoff": num_days_before_cutoff[0:-1],
            "num_days_before_cutoff_from": num_days_before_cutoff[1:],
        }
    )

    df_estimations_relevant = df_estimations_relevant.merge(df_mapping).merge(
        df_estimations_relevant[["menu_year", "menu_week", "total_orders_estimated", "num_days_before_cutoff"]].rename(
            columns={
                "num_days_before_cutoff": "num_days_before_cutoff_from",
                "total_orders_estimated": "total_orders_estimated_from",
            }
        ),
    )

    df_estimations_relevant["diff"] = (
        df_estimations_relevant["total_orders_estimated"] - df_estimations_relevant["total_orders_estimated_from"]
    )
    df_estimations_relevant["x-y"] = (
        df_estimations_relevant["num_days_before_cutoff_from"].astype(int).astype(str)
        + "-"
        + df_estimations_relevant["num_days_before_cutoff"].astype(int).astype(str)
    )
    df_estimations_relevant.sort_values(
        by=["num_days_before_cutoff", "menu_year", "menu_week"], ascending=[True, True, True]
    )
    df_estimations_relevant.pivot(index="x-y", columns=["menu_year", "menu_week"], values="diff")
    df_estimations_relevant = df_estimations_relevant.sort_values(by=["menu_year", "menu_week"])
    pivot_table = df_estimations_relevant.pivot(index="x-y", columns=["menu_year", "menu_week"], values="diff")
    pivot_table = pivot_table.reindex(sorted(pivot_table.index, key=lambda x: int(x.split("-")[0]), reverse=True))

    return pivot_table
