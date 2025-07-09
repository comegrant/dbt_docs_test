# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false

import itertools
from typing import Optional

import numpy as np
import pandas as pd
from project_f.misc import create_date_from_year_week


def get_established_by_start_year(df: pd.DataFrame) -> pd.DataFrame:
    df_established_by_year = pd.DataFrame(
        df.groupby(
            [
                "company_name",
                "current_delivery_year",
                "current_delivery_week",
                "signup_year",
            ]
        )["number_of_agreements"].sum()
    ).reset_index()

    return df_established_by_year


def get_loyalty_group_by_creation_year(df: pd.DataFrame) -> pd.DataFrame:
    df_total_per_year = (
        pd.DataFrame(
            df.groupby(
                [
                    "company_name",
                    "current_delivery_year",
                    "current_delivery_week",
                    "signup_year",
                ]
            )["number_of_agreements"].sum()
        )
        .reset_index()
        .rename(columns={"number_of_agreements": "yearly_sum_all_segments"})
    )

    df = df.merge(
        df_total_per_year,
        on=[
            "company_name",
            "current_delivery_year",
            "current_delivery_week",
            "signup_year",
        ],
    )

    df["segment_share"] = df["number_of_agreements"] / df["yearly_sum_all_segments"]
    df_segment_share = pd.DataFrame(
        df.groupby(["company_name", "current_delivery_year", "signup_year", "customer_journey_sub_segment_name"])[
            "segment_share"
        ].mean()
    ).reset_index()

    return df_segment_share


def calculate_weekly_discount(compounded_rate: float, num_weeks_compounded: int) -> float:
    weekly_discount_rate = np.exp(np.log(compounded_rate) / (num_weeks_compounded - 1))

    return weekly_discount_rate


def create_compound_calendar(
    start_year: int,
    start_week: int,
    end_year: int,
    end_week: int,
) -> pd.DataFrame:
    first_delivery_date_monday = create_date_from_year_week(year=start_year, week=start_week, weekday=1)

    last_included_date = create_date_from_year_week(year=end_year, week=end_week, weekday=6)

    maximum_num_established_weeks = int(np.ceil((last_included_date - first_delivery_date_monday).days / 7))
    num_compounded_weeks = np.arange(maximum_num_established_weeks)
    df_compound_calendar = pd.DataFrame(list(itertools.product([first_delivery_date_monday], num_compounded_weeks)))

    df_compound_calendar.columns = [
        "current_delivery_date_monday",
        "num_compounded_weeks",
    ]
    df_compound_calendar["delivery_date"] = df_compound_calendar["current_delivery_date_monday"] + df_compound_calendar[
        "num_compounded_weeks"
    ].apply(lambda x: pd.Timedelta(weeks=x))
    df_compound_calendar = df_compound_calendar[df_compound_calendar["delivery_date"] < last_included_date]
    df_compound_calendar[["delivery_year", "delivery_week"]] = df_compound_calendar["delivery_date"].dt.isocalendar()[
        ["year", "week"]
    ]
    df_compound_calendar["current_delivery_year"] = start_year
    df_compound_calendar["current_delivery_week"] = start_week

    return df_compound_calendar


def get_established_customers_development(
    df_established_customers: pd.DataFrame,
    df_established_retention_rate: pd.DataFrame,
    end_year: int,
    end_week: int,
    num_weeks_compounded: Optional[int] = 52,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_established_by_year = get_established_by_start_year(df=df_established_customers)
    df_established_by_year = df_established_by_year.sort_values(by=["current_delivery_year", "current_delivery_week"])
    # Only keep the latest year's established customers by starting_year statistics
    df_established_by_year_latest = df_established_by_year.drop_duplicates(
        subset=["company_name", "signup_year"], keep="last"
    )

    df_loyalty_by_creation_year = get_loyalty_group_by_creation_year(df=df_established_customers)

    # Split established customers into loyalty
    df_established_by_year_per_loyalty = df_established_by_year_latest.merge(
        df_loyalty_by_creation_year,
        on=["company_name", "current_delivery_year", "signup_year"],
        how="inner",
    )
    df_established_by_year_per_loyalty["num_customers"] = (
        df_established_by_year_per_loyalty["number_of_agreements"] * df_established_by_year_per_loyalty["segment_share"]
    ).round()

    # Calculate the weekly retention rate
    df_established_retention_rate["weekly_discount_rate"] = df_established_retention_rate["share_still_active"].apply(
        lambda x: calculate_weekly_discount(num_weeks_compounded=num_weeks_compounded, compounded_rate=x)
    )
    df_established_by_year_per_loyalty = df_established_by_year_per_loyalty.merge(
        df_established_retention_rate[["company_name", "signup_year", "weekly_discount_rate"]]
    )

    # One compound calendar per brand: since they can be at different current_delivery_week
    df_compound_calendar_starting_point = df_established_by_year_per_loyalty[
        ["company_name", "current_delivery_year", "current_delivery_week"]
    ].drop_duplicates()
    compound_calendar_ls = []
    for (
        _,
        company_name,
        current_delivery_year,
        current_delivery_week,
    ) in df_compound_calendar_starting_point.itertuples():
        df_compound_calendar = create_compound_calendar(
            start_year=current_delivery_year,
            start_week=current_delivery_week,
            end_year=end_year,
            end_week=end_week,
        )
        df_compound_calendar["company_name"] = company_name
        compound_calendar_ls.append(df_compound_calendar)
    df_compound_calendar = pd.concat(compound_calendar_ls)

    df_established_customers_per_week = df_established_by_year_per_loyalty.merge(df_compound_calendar)

    df_established_customers_per_week["num_customers_retained"] = (
        (
            df_established_customers_per_week["num_customers"]
            * (
                df_established_customers_per_week["weekly_discount_rate"]
                ** df_established_customers_per_week["num_compounded_weeks"]
            )
        )
        .round()
        .astype(int)
    )
    return df_established_customers_per_week, df_established_by_year_per_loyalty


def get_buyer_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """Only works for established customers
    buyer_frequency is defined as # orders / # customers
    """
    df_buyer_frequency = (
        df.groupby(["company_name", "current_delivery_year", "customer_journey_sub_segment_name"])[
            ["number_of_agreements", "number_of_orders"]
        ]
        .sum()
        .reset_index()
    )

    df_buyer_frequency["buyer_frequency"] = (
        df_buyer_frequency["number_of_orders"] / df_buyer_frequency["number_of_agreements"]
    )

    return df_buyer_frequency


def get_established_customers_orders(
    df_established_customers_per_week: pd.DataFrame, df_buyer_frequency: pd.DataFrame
) -> pd.DataFrame:
    df_buyer_frequency_latest = df_buyer_frequency[
        df_buyer_frequency["current_delivery_year"] == df_buyer_frequency["current_delivery_year"].max()
    ]

    df_established_orders_per_week = df_established_customers_per_week.merge(
        df_buyer_frequency_latest[["company_name", "customer_journey_sub_segment_name", "buyer_frequency"]]
    )

    df_established_orders_per_week["number_of_orders"] = (
        (df_established_orders_per_week["buyer_frequency"] * df_established_orders_per_week["num_customers_retained"])
        .round()
        .astype(int)
    )

    df_established_final = pd.DataFrame(
        df_established_orders_per_week.groupby(["company_name", "delivery_year", "delivery_week"])[
            "number_of_orders"
        ].sum()
    ).reset_index()

    return df_established_final


def calculate_orders_established(
    df_established_customers: pd.DataFrame,
    df_established_retention_rate: pd.DataFrame,
    end_year: int,
    end_week: int,
    num_weeks_compounded: Optional[int] = 52,
) -> pd.DataFrame:
    (
        df_established_customers_per_week,
        df_established_by_year_per_loyalty,
    ) = get_established_customers_development(
        df_established_customers=df_established_customers,
        df_established_retention_rate=df_established_retention_rate,
        end_year=end_year,
        end_week=end_week,
        num_weeks_compounded=num_weeks_compounded,
    )
    df_buyer_frequency = get_buyer_frequency(df=df_established_customers)
    df_established_final = get_established_customers_orders(
        df_established_customers_per_week=df_established_customers_per_week,
        df_buyer_frequency=df_buyer_frequency,
    )

    return df_established_final, df_established_by_year_per_loyalty
