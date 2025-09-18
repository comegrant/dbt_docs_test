"""
TO BE DEPRECATED: This is the old menu optimiser.
"""

import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer


def exclude_recipes(
    df: pd.DataFrame,
    min_rating: int,
) -> pd.DataFrame:
    """
    TO BE DEPRECATED.
    Exclude recipes with average rating below the threshold. Missing ratings are filled with min_rating.
    """

    df["average_rating"] = df["average_rating"].fillna(min_rating)

    return pd.DataFrame(df[df["average_rating"] >= min_rating])


def get_dummies(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    TO BE DEPRECATED.
    Creates a new 0's and 1's df. Used to count constraints in the recipes.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame of recipes.

    Returns
    -------
    pd.DataFrame
        The DataFrame of recipes with constraint columns.
    """

    main_dummies = pd.get_dummies(df["main_ingredient_id"], prefix="main_ingredient_id")
    price_dummies = pd.get_dummies(df["price_category_id"], prefix="price_category_id")
    time_dummies = pd.get_dummies(df["cooking_time_to"], prefix="cooking_time_to")
    catecorical_dummies = pd.concat([main_dummies, price_dummies, time_dummies], axis=1)

    mlb = MultiLabelBinarizer()
    taxonomy_dummies = pd.DataFrame(
        mlb.fit_transform(df["taxonomies"]), columns=mlb.classes_, index=df.index
    )
    taxonomy_dummies = taxonomy_dummies.add_prefix("taxonomy_id_")

    dff = pd.concat(
        [
            df[["recipe_id", "is_universe"]],
            taxonomy_dummies,
            catecorical_dummies,
        ],
        axis=1,
    )

    return dff


def get_distribution(
    items,
    df,
    not_found,
    field_name,
):
    """
    TO BE DEPRECATED.
    Get the difference between the number of wanted and actual recipes with the given items.

    Example:
        ings_out, ings_not_full = get_distribution(
        ingredients,
        final_df,
        ings_not_found,
        field_name="main_ingredient_id",
        wanted_field="quantity",
        actual_field="actual",
    )
    """

    output_list = []
    not_full = []

    for item in items:
        df_column = f"{field_name}_{getattr(item, field_name)}"

        skip = getattr(item, field_name) in not_found
        actual = 0 if skip else df[df_column].sum()

        output = {field_name: getattr(item, field_name)}
        output["wanted"] = getattr(item, "quantity")
        output["actual"] = int(actual)
        output_list.append(output)
        if output["wanted"] > output["actual"]:
            # TODO: not sure if this always works as intended, double check
            not_full.append(output[field_name])
    return output_list, not_full


def get_priority_df(
    df: pd.DataFrame,
    dist: dict[str, int | float],
    recipes: list[list[int]],
    just_fill: bool = False,
) -> pd.DataFrame:
    """
    TO BE DEPRECATED.
    Generate a priority DataFrame based on overlay calculations and specified recipes.

    Parameters:
    ------------
    df (DataFrame): The input DataFrame containing the data to be processed.
    dist (Dict[str, Union[int, float]]): A dictionary where keys are column names and values are weights
    used to calculate overlays.
    recipes (List[List[int]]): A list of lists, where each inner list contains indices representing a recipe.
    just_fill (bool): A flag indicating whether to use the entire DataFrame (True) or only the subset with the maximum
    number of overlays (False). Default is False.

    Returns:
    ----------
    DataFrame: A DataFrame containing the priority rows based on the given recipes and overlay calculations.
    If the input DataFrame is empty, returns an empty DataFrame.
    """
    # Check if the input DataFrame is empty
    if len(df) == 0:
        print("In get_priority_df: No recipes available")
        return pd.DataFrame()  # Return an empty DataFrame if input is empty

    # Calculate the number of overlays for each row based on the columns specified in dist
    df["n_overlay"] = df[list(dist.keys())].sum(axis=1) - 1

    # Initialize an empty DataFrame to store the priority rows
    priority_df = pd.DataFrame()
    i = 0

    # Select the subset of the DataFrame with the maximum number of overlays
    # If just_fill is True, use the entire DataFrame
    mult_df = df[df["n_overlay"] == max(df["n_overlay"])] if not just_fill else df

    # Iterate through the recipes to find the first non-empty priority DataFrame
    while len(priority_df) == 0 and i < len(recipes):
        # Select rows from mult_df that match the current recipe indices
        priority_df = mult_df[mult_df.index.isin(recipes[i])]
        i += 1  # Move to the next recipe

    # Return the priority DataFrame
    return pd.DataFrame(priority_df)


def update_dist(  # TODO: seems unnecessary to do it this way
    dist: dict[str, float], selected_df: pd.DataFrame, available_df: pd.DataFrame
) -> tuple[dict[str, float], pd.DataFrame]:
    """
    TO BE DEPRECATED.
    Updates the distribution of key quantities based on the given DataFrame and returns the updated distribution
    and the modified wanted DataFrame.

    Args:
        dist (Dict[str, float]): A dictionary where keys are key names and values are their quantities.
        df (pd.DataFrame): A DataFrame containing the quantities to be subtracted from the distribution.
        wanted_df (pd.DataFrame): A DataFrame from which the rows corresponding to the indices of df will be dropped.

    Returns:
        Tuple[Dict[str, float], pd.DataFrame]: A tuple containing the updated distribution dictionary
        and the modified wanted DataFrame.
    """
    # Initialize an empty dictionary to store the updated distribution
    actual = {}

    # Drop the rows from available_df that correspond to the indices in df
    available_df = available_df.drop(list(selected_df.index))

    # Iterate over each key in the distribution dictionary
    for key in dist:
        # Calculate the new quantity by subtracting the sum of the quantities
        #  in df from the current quantity in dist
        new_quantity = dist[key] - selected_df[key].sum()

        # If the new quantity is zero, skip adding it to the actual dictionary
        if not new_quantity:
            continue

        # Update the actual dictionary with the new quantity
        actual[key] = new_quantity

    # Return the updated distribution dictionary and the modified wanted DataFrame
    return actual, available_df


def get_message(
    ings_not_full: list[str],
    prices_not_full: list[str],
    cooking_times_not_full: list[str],
    ings_not_found: list[str],
    prices_not_found: list[str],
    cooking_times_not_found: list[str],
    recipes_not_full: bool,
    taxonomy_id: int,
) -> tuple[int, str]:
    """
    TO BE DEPRECATED.
    Generates a status message based on the completeness and availability of ingredients, prices,
    and cooking times for recipes.

    Args:
        ings_not_full (list): List of ingredients that are not fully available.
        prices_not_full (list): List of price categories that are not fully available.
        cooking_times_not_full (list): List of cooking times that are not fully available.
        ings_not_found (list): List of ingredients that were not found in the recipe bank.
        prices_not_found (list): List of price categories that were not found in the recipe bank.
        cooking_times_not_found (list): List of cooking times that were not found in the recipe bank.
        recipes_not_full (bool): Flag indicating if there are not enough recipes.
        taxonomy_id (int): The taxonomy identifier for the recipes.

    Returns:
        tuple: A tuple containing a status code (0 for success, 1 for warning) and a message string.
    """
    # Check if all lists are empty and there are enough recipes
    if (
        not len(ings_not_full + prices_not_full + cooking_times_not_full)
        and not recipes_not_full
    ):
        return (
            0,
            f"TAXONOMY {taxonomy_id} SUCCESS.",
        )  # Return success message if all conditions are met

    msg = ""  # Initialize the message string

    # Append messages for ingredients not found
    if len(ings_not_found):
        msg += f"INGREDIENTS {str(ings_not_found)[1:-1]} not found on recipe bank."

    # Append messages for price categories not found
    if len(prices_not_found):
        msg += f"RECIPES WITH PRICES CATEGORIES {prices_not_found} not found on recipe bank."

    # Append messages for cooking times not found
    if len(cooking_times_not_found):
        msg += f"RECIPES WITH COOKING TIMES {cooking_times_not_found} not found on recipe bank."

    # Append message if there are not enough recipes
    if recipes_not_full:
        msg += "NOT ENOUGH RECIPES."

    # Append messages for ingredients not fully available
    if len(ings_not_full):
        msg += f"INGREDIENTS {str(ings_not_full)[1:-1]} not fulfilled."

    # Append messages for price categories not fully available
    if len(prices_not_full):
        msg += f"RECIPES WITH PRICES CATEGORIES {prices_not_full} not fulfilled."

    # Append messages for cooking times not fully available
    if len(cooking_times_not_full):
        msg += f"RECIPES WITH COOKING TIMES < {cooking_times_not_full} not fulfilled."

    # Return warning message with the constructed message string
    return (1, f"TAXONOMY {taxonomy_id} WARNING! {msg}")
