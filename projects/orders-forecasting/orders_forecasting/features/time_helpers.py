import pandas as pd


def add_lag_features(
    df: pd.DataFrame,
    origin_col: str,
    lag_list: list[int] | None = None,
    prefix: str | None = "lag_",
) -> pd.DataFrame:
    if isinstance(lag_list, int):
        lag_list = [lag_list]
    if isinstance(lag_list, list):
        df = df.sort_values(by=["year", "week"])
        for lag in lag_list:
            lag_col_name = prefix + str(lag)
            df[lag_col_name] = df[origin_col].shift(lag)
            # df[lag_col_name] = df[lag_col_name].astype(int)
    return df


def add_moving_avg_features(
    df: pd.DataFrame,
    origin_col: str,
    window_list: list[int] | None = None,
    prefix: str | None = "moving_avg_",
) -> pd.DataFrame:
    if isinstance(window_list, int):
        window_list = [window_list]
    if isinstance(window_list, list):
        df = df.sort_values(by=["year", "week"])
        for window in window_list:
            mv_col_name = prefix + str(window)
            df[mv_col_name] = df[origin_col].shift(1).rolling(window).mean()
    return df
