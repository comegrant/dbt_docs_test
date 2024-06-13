import pandas as pd

from dishes_forecasting.time_machine import has_year_week_53


def clean_weekly_dishes(df_weekly_dishes: pd.DataFrame) -> pd.DataFrame:
    df_weekly_dishes = remove_invalid_week_53(df_weekly_dishes)
    df_weekly_dishes["flex_menu_recipe_order"] = (
        df_weekly_dishes["flex_menu_recipe_order"].fillna(0).astype(int)
    )
    df_weekly_dishes["is_default"] = (
        df_weekly_dishes["is_default"].fillna(0).astype(int)
    )
    df_weekly_dishes = (
        df_weekly_dishes.sort_values(by=["year", "week"])
        .reset_index()
        .drop(columns="index")
    )
    return df_weekly_dishes


def clean_recipes(
    df_recipes: pd.DataFrame,
    number_of_recipe_steps_default: int,
    cooking_time_from_default: int,
    cooking_time_to_default: int,
) -> pd.DataFrame:
    df_recipes["number_of_recipe_steps"] = (
        df_recipes["number_of_recipe_steps"]
        .fillna(number_of_recipe_steps_default)
        .astype(int)
    )

    df_recipes["cooking_time_from"] = (
        df_recipes["cooking_time_from"].fillna(cooking_time_from_default).astype(int)
    )

    df_recipes["cooking_time_to"] = (
        df_recipes["cooking_time_to"].fillna(cooking_time_to_default).astype(int)
    )
    return df_recipes


def clean_recipe_price_ratings(
    df_recipe_price_ratings: pd.DataFrame,
    average_recipe_rating_default: float,
    num_recipe_rating_default: int,
) -> pd.DataFrame:
    df_recipe_price_ratings["average_recipe_rating"] = (
        df_recipe_price_ratings["average_recipe_rating"]
        .fillna(average_recipe_rating_default)
        .astype(float)
    )

    df_recipe_price_ratings["num_recipe_rating"] = (
        df_recipe_price_ratings["num_recipe_rating"]
        .fillna(num_recipe_rating_default)
        .astype(int)
    )
    df_recipe_price_ratings = (
        df_recipe_price_ratings.sort_values(by=["year", "week"])
        .reset_index()
        .drop(columns="index")
    )

    return df_recipe_price_ratings


def remove_invalid_week_53(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """If week 53 exists in df but should not exist in real life, remove it

    Args:
        df (pd.DataFrame): a dataframe, must contain columns
            - year (int)
            - week (int)

    Returns:
        pd.DataFrame: the same dataframe with the invalid week 53 removed
    """
    # Find the index of the rows where the week is 53
    # but the year should not have week 53
    max_week = 53
    indices_to_drop = (
        df[df["week"] == max_week]["year"]
        .apply(lambda x: has_year_week_53(x) is False)
        .index
    )
    if len(indices_to_drop) > 0:
        df = df.drop(indices_to_drop)

    return df
