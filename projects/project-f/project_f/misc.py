from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st


def override_file(df_new: pd.DataFrame, original_file_path: Path) -> None:
    if original_file_path.exists():
        if str(original_file_path).endswith(".tsv"):
            df_original = pd.read_csv(original_file_path, sep="\t")
        else:
            df_original = pd.read_csv(original_file_path)
    else:
        df_original = pd.DataFrame(columns=df_new.columns)
    df_overrided = override_df(df_original=df_original, df_new=df_new)
    if str(original_file_path).endswith(".tsv"):
        df_overrided.to_csv(original_file_path, index=False, sep="\t")
    elif str(original_file_path).endswith(".csv"):
        df_overrided.to_csv(original_file_path, index=False)


def override_df(df_new: pd.DataFrame, df_original: pd.DataFrame) -> pd.DataFrame:  # type: ignore
    # First check if columns match
    if not (df_new.columns == df_original.columns).all():
        raise ValueError("Columns do not match.")
    else:
        company_names = df_new["company_name"].unique()
        df_not_override = df_original[~df_original["company_name"].isin(company_names)]  # type: ignore
        df = pd.concat([df_new, df_not_override])
        return df  # type: ignore


@st.cache_data
def convert_df(df: pd.DataFrame) -> bytes:
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


def add_download_csv_button(
    df: pd.DataFrame,
    file_name: str,
) -> None:
    csv = convert_df(df=df)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name=file_name,
        mime="text/csv",
    )


# Define the year, week, and weekday
def create_date_from_year_week(year: int, week: int, weekday: Optional[int] = 1) -> datetime:
    # Create a date based on year, week, and weekday
    a_date = pd.to_datetime(str(year) + "-W" + str(week) + "-" + str(weekday), format="%Y-W%W-%w")

    return a_date


def create_week_calendar(
    start_year: int,
    start_week: int,
    end_year: int,
    end_week: int,
) -> pd.DataFrame:
    start_date = create_date_from_year_week(year=start_year, week=start_week, weekday=1)
    end_date = create_date_from_year_week(year=end_year, week=end_week, weekday=1)
    df_weeks = pd.DataFrame(
        {
            "monday_of_week": pd.date_range(
                # force it to be 1 day before the the first day of the month
                # to include the current month
                start=pd.to_datetime(start_date),
                end=end_date,
                freq="7D",
                inclusive="both",
            )
        }
    )

    df_weeks[["iso_year", "iso_week"]] = df_weeks["monday_of_week"].dt.isocalendar()[["year", "week"]]
    df_weeks["month"] = df_weeks["monday_of_week"].dt.month
    df_weeks["year"] = df_weeks["monday_of_week"].dt.year

    return df_weeks[["year", "month", "monday_of_week", "iso_year", "iso_week"]]  # type: ignore


# Add a date column
def add_date_col_from_year_week_cols(
    df: pd.DataFrame,
    year_colname: Optional[str] = "year",
    week_colname: Optional[str] = "week",
    day_of_week: Optional[int] = 1,
    new_col: Optional[str] = "date",
) -> datetime:
    # Create a date based on year, week, and weekday
    df[new_col] = pd.to_datetime(
        (df[year_colname].astype(str) + "-W" + df[week_colname].astype(str) + f"-{day_of_week!s}"),
        format="%G-W%V-%u",
    )

    return df  # type: ignore
