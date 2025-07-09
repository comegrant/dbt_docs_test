# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false

import pandas as pd


def make_quarter_summary(
    df_historical_budget: pd.DataFrame,
    df_budget_latest: pd.DataFrame,
    df_calendar: pd.DataFrame,
    budget_type: str,
    budget_start: int,
) -> pd.DataFrame:
    df_historical_budget["year_quarter"] = (
        df_historical_budget["year"].astype(str) + " - " + "Q" + df_historical_budget["quarter"].astype(int).astype(str)
    )

    df_historical_budget["budget_year_type"] = (
        df_historical_budget["updated_year"].astype(str) + " - " + df_historical_budget["budget_type_name"]
    )

    df_quarter_summary_history = (
        pd.DataFrame(
            df_historical_budget.groupby(["company_name", "budget_year_type", "year_quarter"])["total_orders"].sum()
        )
        .reset_index()
        .rename(columns={"total_orders": "number_of_orders"})
    )

    df_budget_latest = df_budget_latest.rename(
        columns={
            "delivery_year": "iso_year",
            "delivery_week": "iso_week",
        }
    )

    df_budget_latest_merged = df_budget_latest.merge(
        df_calendar[["iso_year", "iso_week", "year", "week", "quarter"]],
        how="left",
    )

    df_budget_latest_merged["year_quarter"] = (
        df_budget_latest_merged["year"].astype(str)
        + " - "
        + "Q"
        + df_budget_latest_merged["quarter"].astype(int).astype(str)
    )

    df_quarter_summary_latest = pd.DataFrame(
        df_budget_latest_merged.groupby(["company_name", "year_quarter"])["number_of_orders"].sum()
    ).reset_index()

    budget_start_year = budget_start // 100
    df_quarter_summary_latest["budget_year_type"] = str(budget_start_year) + " - " + budget_type
    df_quarter_summary_latest = df_quarter_summary_latest[df_quarter_summary_history.columns]

    df_quarter_summary = pd.concat([df_quarter_summary_latest, df_quarter_summary_history])
    return df_quarter_summary


def prep_df_for_historical_budget_plot(
    df_latest_budget: pd.DataFrame,
    df_historical_budget: pd.DataFrame,
    budget_type: str,
    budget_start: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_historical_budget["budget_year_type"] = (
        df_historical_budget["year"].astype(str) + " - " + df_historical_budget["budget_type_name"]
    )
    budget_start_year = budget_start // 100

    df_latest_budget_agg = pd.DataFrame(
        df_latest_budget.groupby(
            [
                "company_name",
                "delivery_year",
                "delivery_week",
            ]
        )["number_of_orders"].sum()
    ).reset_index()

    df_latest_budget_agg["budget_year_type"] = str(budget_start_year) + " - " + budget_type

    df_historical_budget_agg = (
        pd.DataFrame(
            df_historical_budget.groupby(["company_name", "year", "week", "budget_year_type"])["total_orders"].sum()
        )
        .reset_index()
        .rename(
            columns={
                "year": "delivery_year",
                "week": "delivery_week",
                "total_orders": "number_of_orders",
            }
        )
    )

    df_historical_budget_agg = df_historical_budget_agg[df_latest_budget_agg.columns]

    df_budget_combined = pd.concat([df_historical_budget_agg, df_latest_budget_agg])
    df_budget_combined = df_budget_combined[
        df_budget_combined["delivery_year"] * 100 + df_budget_combined["delivery_week"] >= budget_start
    ]

    df_fake_week_numbers = df_budget_combined[["delivery_year", "delivery_week"]].drop_duplicates().reset_index()

    df_fake_week_numbers["week_index"] = df_fake_week_numbers.index
    df_budget_to_plot = df_budget_combined.merge(df_fake_week_numbers[["delivery_year", "delivery_week", "week_index"]])

    df_budget_to_plot = df_budget_to_plot.sort_values(by="week_index")
    df_budget_to_plot["year_week"] = (
        df_budget_to_plot["delivery_year"].astype(str) + "-" + df_budget_to_plot["delivery_week"].astype(str)
    )

    return df_budget_to_plot, df_fake_week_numbers
