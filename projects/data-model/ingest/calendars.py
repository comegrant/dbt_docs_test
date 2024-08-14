import pandas as pd


def get_calendar_dataframe(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Returns a DataFrame containing a calendar with a range of dates between the specified start and end dates.

    Parameters:
    - start_date (str): The start date of the calendar in the format 'YYYY-MM-DD'.
    - end_date (str): The end date of the calendar in the format 'YYYY-MM-DD'.

    Returns:
    - pd.DataFrame: A DataFrame containing the calendar with the following columns:
        - date (datetime64): The date of each day in the calendar.
        - year (int): The year of each day in the calendar.
        - week (int): The week number of each day in the calendar.
        - datekey (int): The date in integer format, obtained by removing dashes from the date string.
        - day_of_week (int): The day of the week (0 = Monday, 6 = Sunday) of each day in the calendar.
        - weekday_name (str): The name of the weekday (e.g., Monday, Tuesday) of each day in the calendar.
        - quarter (int): The quarter of the year (1-4) of each day in the calendar.
        - month_number (int): The month number (1-12) of each day in the calendar.
        - month_name (str): The name of the month (e.g., January, February) of each day in the calendar.
    """

    if pd.to_datetime(end_date) < pd.to_datetime(start_date):
        raise ValueError("End date cannot be before start date.")

    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"date": date_range})

    df["year"] = df["date"].dt.isocalendar().year
    df["week"] = df["date"].dt.isocalendar().week
    df["datekey"] = df["date"].astype(str).str.replace("-", "").astype(int)
    df["day_of_week"] = df["date"].dt.dayofweek
    df["weekday_name"] = df["date"].dt.day_name()
    df["quarter"] = df["date"].dt.quarter
    df["month_number"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.month_name()

    return df
