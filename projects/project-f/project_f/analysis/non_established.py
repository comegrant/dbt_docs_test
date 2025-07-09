# pyright: reportArgumentType=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportIndexIssue=false

import pandas as pd
from project_f.misc import add_date_col_from_year_week_cols


def calculate_historical_aquisition_one_segment(
    df_historical_cohort: pd.DataFrame,
    df_calendar: pd.DataFrame,
    customer_segment_name: str,
) -> pd.DataFrame:
    df_1st_week = df_historical_cohort[df_historical_cohort["cohort_week_order"] == 1]
    df_1st_week["iso_year"] = df_1st_week["start_delivery_year_week"].apply(lambda x: int(x[:4]))
    df_1st_week["iso_week"] = df_1st_week["start_delivery_year_week"].apply(lambda x: int(x[-2:]))

    df_merged_with_calendar = df_1st_week.merge(df_calendar)

    df_aggregated = (
        pd.DataFrame(
            df_merged_with_calendar.groupby(["company_name", "year", "month", "source"])[
                "number_of_registrations"
            ].sum()
        )
        .reset_index()
        .rename(columns={"number_of_registrations": "estimate"})
    )
    df_aggregated["customer_segment"] = customer_segment_name
    return df_aggregated


def get_historical_acquisition_all_segment(
    df_new_customers: pd.DataFrame,
    df_reactivated_customers: pd.DataFrame,
    df_calendar: pd.DataFrame,
) -> pd.DataFrame:
    df_historical_acquisition_new = calculate_historical_aquisition_one_segment(
        df_historical_cohort=df_new_customers,
        df_calendar=df_calendar,
        customer_segment_name="New Customers",
    )

    df_historical_acquisition_reactivated = calculate_historical_aquisition_one_segment(
        df_historical_cohort=df_reactivated_customers[df_reactivated_customers["is_with_discount"] == "yes"],
        df_calendar=df_calendar,
        customer_segment_name="Reactivated Customers",
    )

    df_historical_acquisition_reactivated_no_code = calculate_historical_aquisition_one_segment(
        df_historical_cohort=df_reactivated_customers[df_reactivated_customers["is_with_discount"] != "yes"],
        df_calendar=df_calendar,
        customer_segment_name="Reactivated Customers No Code",
    )

    df_historical_acquisition = pd.concat(
        [
            df_historical_acquisition_new,
            df_historical_acquisition_reactivated,
            df_historical_acquisition_reactivated_no_code,
        ],
        ignore_index=True,
    )

    return df_historical_acquisition


def analyze_cohort(
    df: pd.DataFrame,
    df_calendar: pd.DataFrame,
) -> pd.DataFrame:
    df["iso_year"] = df["start_delivery_year_week"].apply(lambda x: int(x[:4]))
    df["iso_week"] = df["start_delivery_year_week"].apply(lambda x: int(x[-2:]))

    # Merge to get month
    df = df.merge(df_calendar[["year", "month", "week", "iso_year", "iso_week"]])

    df_total_registrations = df[df["cohort_week_order"] == 1]

    df_total_registrations_agg = pd.DataFrame(
        df_total_registrations.groupby(
            [
                "company_name",
                "year",
                "month",
            ]
        )["number_of_registrations"].sum()
    ).reset_index()

    df_cohorts = pd.DataFrame(
        df.groupby(
            [
                "company_name",
                "year",
                "month",
                "cohort_week_order",
            ]
        )["number_of_orders"].sum()
    ).reset_index()

    df_cohorts = df_cohorts.merge(df_total_registrations_agg)

    df_cohorts["average_orders"] = df_cohorts["number_of_orders"] / df_cohorts["number_of_registrations"]
    df_cohorts = df_cohorts.sort_values(by=["company_name", "year", "month", "cohort_week_order"])

    df_cohorts = df_cohorts.drop(columns=["number_of_registrations", "number_of_orders"])

    return df_cohorts


def calculate_general_cohort(
    df_new_customers: pd.DataFrame,
    df_reactivated_customers: pd.DataFrame,
    df_calendar: pd.DataFrame,
) -> pd.DataFrame:
    df_reactivated_customers_no_code = df_reactivated_customers[df_reactivated_customers["is_with_discount"] != "yes"]
    df_reactivated_customers_with_code = df_reactivated_customers[df_reactivated_customers["is_with_discount"] == "yes"]

    customer_segment_names = [
        "New Customers",
        "Reactivated Customers",
        "Reactivated Customers No Code",
    ]
    dfs = [
        df_new_customers,
        df_reactivated_customers_with_code,
        df_reactivated_customers_no_code,
    ]
    cohort_dfs = []
    for df, segment_name in zip(dfs, customer_segment_names):
        if df.shape[0] >= 1:
            df_cohort = analyze_cohort(df=df, df_calendar=df_calendar)
            df_cohort["customer_segment"] = segment_name
            cohort_dfs.append(df_cohort)

    df_cohorts = pd.concat(cohort_dfs)
    df_cohorts = df_cohorts.rename(columns={"cohort_week_order": "cohort_week"})
    df_cohorts = df_cohorts[
        [
            "company_name",
            "year",
            "month",
            "customer_segment",
            "cohort_week",
            "average_orders",
        ]
    ]

    return df_cohorts  # type: ignore


def keep_latest_cohorts(df_cohorts: pd.DataFrame, num_months: int) -> pd.DataFrame:
    df_cohort_list = []
    for company_name in df_cohorts.company_name.unique():
        for customer_segment in df_cohorts.customer_segment.unique():
            df_one_cohort = df_cohorts[
                (df_cohorts["company_name"] == company_name) & (df_cohorts["customer_segment"] == customer_segment)
            ]

            year_month_with_full_cohorts = df_one_cohort[
                df_one_cohort["cohort_week"] == df_one_cohort["cohort_week"].max()
            ][["year", "month"]].drop_duplicates()
            latest_year_month_with_full_cohorts = year_month_with_full_cohorts.sort_values(by=["year", "month"]).tail(  # type: ignore
                num_months
            )
            df_one_cohort = df_one_cohort.merge(latest_year_month_with_full_cohorts)

            df_cohort_list.append(df_one_cohort)
    df_cohort_latest = pd.concat(df_cohort_list)
    df_cohort_latest = df_cohort_latest.sort_values(by=["company_name", "customer_segment", "month", "cohort_week"])
    return df_cohort_latest


def get_start_delivery(
    df_marketing_input: pd.DataFrame,
    df_weeks_until_start: pd.DataFrame,
    df_calendar: pd.DataFrame,
) -> pd.DataFrame:
    df_merged = df_marketing_input.merge(df_calendar[["year", "month", "week", "monday_date", "iso_week"]])
    df_num_weeks_in_month = (
        pd.DataFrame(df_calendar.groupby(["year", "month"])["iso_week"].nunique())
        .reset_index()
        .rename(columns={"iso_week": "num_weeks_in_month"})
    )
    df_merged = df_merged.merge(df_num_weeks_in_month)
    # Assume that customers join each week evenly
    df_merged["num_joined_on_week"] = df_merged["estimate"] / df_merged["num_weeks_in_month"]
    df_merged = df_merged.merge(df_weeks_until_start, on=["source", "company_name"])
    # calculate the start delivery year, week
    # use time delta * 7D instead of adding up week number so that it still works when a year ends
    df_merged[["start_delivery_year", "start_delivery_week"]] = (
        pd.to_datetime(df_merged["monday_date"])
        + df_merged["weeks_til_start"].apply(lambda x: pd.Timedelta(days=7 * x))  # would be iso weeks
    ).dt.isocalendar()[["year", "week"]]
    # calculate the split to each start week
    df_merged["num_started_delivery"] = (df_merged["percent_split"] * df_merged["num_joined_on_week"]).astype(int)
    df_started_delivery = df_merged[
        [
            "company_name",
            "year",
            "month",
            "source",
            "customer_segment",
            "start_delivery_year",
            "start_delivery_week",
            "num_started_delivery",
        ]
    ]

    # Do a bit of aggregation
    df_started_delivery = pd.DataFrame(
        df_merged.groupby(
            [
                "company_name",
                "source",
                "year",
                "month",
                "customer_segment",
                "start_delivery_year",
                "start_delivery_week",
            ]
        )["num_started_delivery"].sum()
    ).reset_index()

    return df_started_delivery


def calculate_orders(
    df_cohort_latest: pd.DataFrame,
    df_started_delivery: pd.DataFrame,
    df_source_effect: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Split the cohort by payment method
    df_started_delivery_merged = df_started_delivery.merge(df_source_effect)
    # merge with cohort
    df_started_delivery_merged = df_started_delivery_merged.merge(
        df_cohort_latest[
            [
                "company_name",
                "month",
                "customer_segment",
                "cohort_week",
                "average_orders",
            ]
        ],
        on=["company_name", "month", "customer_segment"],
    )
    df_started_delivery_merged["number_of_orders"] = (
        df_started_delivery_merged["num_started_delivery"]
        * df_started_delivery_merged["average_orders"]
        * df_started_delivery_merged["source_effect"]
    ).astype(int)
    # Get the actual delivery year and week
    df_started_delivery_merged = add_date_col_from_year_week_cols(
        df=df_started_delivery_merged,
        year_colname="start_delivery_year",
        week_colname="start_delivery_week",
        day_of_week=1,
        new_col="start_delivery_date_monday",
    )
    df_started_delivery_merged[["delivery_year", "delivery_week"]] = (
        df_started_delivery_merged["start_delivery_date_monday"]
        + (df_started_delivery_merged["cohort_week"].apply(lambda x: pd.Timedelta(days=7 * (x - 1))))
    ).dt.isocalendar()[["year", "week"]]

    df_orders = pd.DataFrame(
        df_started_delivery_merged.groupby(
            [
                "company_name",
                "year",
                "month",
                "source",
                "customer_segment",
                "delivery_year",
                "delivery_week",
            ]
        )["number_of_orders"].sum()
    ).reset_index()

    return df_started_delivery_merged, df_orders  # type: ignore


def summarize_non_established_customers(
    df_non_established_orders: pd.DataFrame, df_marketing_input: pd.DataFrame
) -> pd.DataFrame:
    df_non_established_summary = (
        pd.DataFrame(
            df_non_established_orders.groupby(["company_name", "year", "month", "customer_segment", "source"])[
                "number_of_orders"
            ].sum()
        )
        .reset_index()
        .rename(columns={"number_of_orders": "total_orders_8_weeks"})
    )

    df_non_established_summary = df_non_established_summary.merge(df_marketing_input).rename(
        columns={"estimate": "number_of_customers"}
    )

    df_non_established_summary["avg_orders_8_weeks"] = (
        df_non_established_summary["total_orders_8_weeks"] / df_non_established_summary["number_of_customers"]
    )

    return df_non_established_summary
