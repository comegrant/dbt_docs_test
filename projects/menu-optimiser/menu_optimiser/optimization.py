import logging
import os
import sys
from typing import Any

import pandas as pd
from data_contracts.helper import camel_to_snake  # type: ignore
from menu_optimiser.utils import input_recipes

sys.path.append(os.getcwd())  # noqa: PTH109


from menu_optimiser.data import (
    get_cooking_times_distribution,
    get_ingredient_distribution,
    get_prices_distribution,
    postprocess_recipe_data,
)
from menu_optimiser.utils.constants import (
    CONSTRAINT_COLUMN,
    COOKING_TIME_COLUMN,
    COOKING_TIME_FROM_COLUMN,
    COOKING_TIME_TO_COLUMN,
    ING_COLUMN,
    PRICE_CATEGORY_COLUMN,
    RATING_COLUMN,
    RECIPE_ID_COLUMN,
    TAX_COLUMN,
    UNIVERSE_COLUMN,
)

logger = logging.getLogger(__name__)


def exclude_recipes(
    df: pd.DataFrame,
    min_rating: int,
) -> pd.DataFrame:
    """
    Exclude recipes based on rating, recipes with no rating are not excluded.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame of recipes
    min_rating : int
        Minimum rating to include recipes

    Returns
    -------
    pd.DataFrame
        DataFrame of recipes filtered by rating
    """

    # df_ing = df_ing[df_ing["ingredient_id"].isin(ingredients)]

    df.loc[:, RATING_COLUMN] = df[RATING_COLUMN].fillna(min_rating)

    df_after_excluded = df[df[RATING_COLUMN] >= min_rating]

    # df_after_excluded = df_after_excluded[df_after_excluded[UNIVERSE_COLUMN]]

    return df_after_excluded  # pyright: ignore


def assign_interval(
    value: int | float, intervals: list[tuple[int | float, int | float]]
) -> str:
    """
    Assigns a value to an interval. If the value is not within any interval, returns "Other".

    Parameters
    ----------
    value : int | float
        The value to be assigned.
    intervals : list[tuple[int | float, int | float]]
        A list of intervals. Each interval is a tuple of two values, the start and end of the interval.

    Returns
    -------
    str
        The name of the interval, or "Other" if the value is not within any interval.
    """
    for _, interval in enumerate(intervals):
        if interval[0] <= value <= interval[1]:
            return interval  # type: ignore  # pyright: ignore
    return "Other"


def get_dummies(
    df: pd.DataFrame,
    df_grouped: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates a new 0's and 1's df. Used to count constraints in the recipes.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame of recipes.
    df_grouped : pd.DataFrame
        The grouped DataFrame of recipes.

    Returns
    -------
    pd.DataFrame
        The DataFrame of recipes with constraint columns.
    """

    dummies = pd.get_dummies(
        data=df,
        columns=[
            ING_COLUMN,
            PRICE_CATEGORY_COLUMN,
            COOKING_TIME_COLUMN,
        ],  # Constraints columns
    )

    dummies_df = (
        df[[RECIPE_ID_COLUMN]]
        .set_index(RECIPE_ID_COLUMN)
        .join(
            dummies.drop([RATING_COLUMN, UNIVERSE_COLUMN], axis=1).set_index(
                RECIPE_ID_COLUMN
            )
        )
    )

    out = dummies_df.groupby([RECIPE_ID_COLUMN]).sum()
    out = out.map(
        lambda x: 1 if x > 0 else x
    )  # To avoid data errors, for example duplicated taxonomies in a recipe, this way the same constraint is not count twice or more times.  # noqa: E501
    out = out.join(df_grouped)
    # out["n_overlay"] = out[list(dist.keys())].sum(axis=1) - 1
    return out


def get_priorities(df_recipes: pd.DataFrame) -> list[list[int]]:
    """
    Get recipes ordered by universe and ingredients.

    Parameters
    ----------
    df_recipes : pd.DataFrame
        DataFrame of recipes.

    Returns
    -------
    List[List[int]]
        List of lists of recipe IDs, where the first list contains the priority recipes with isUniverse
        flag, and the second list contains the priority recipes with ingredients.
    """
    # Priorities: First isUniverse, then ingredients.

    recipes_ordered = []

    for is_universe in (True, False):  # True is Priority
        df_universe = df_recipes[df_recipes[UNIVERSE_COLUMN] == is_universe]
        recipes = list(df_universe[RECIPE_ID_COLUMN].unique())  # type: ignore
        recipes_ordered.append(recipes)

    recipes_ordered = list(filter(None, recipes_ordered))
    return recipes_ordered


def get_priority_df(
    df: pd.DataFrame,
    dist: dict[str, int | float],
    recipes: list[list[int]],
    just_fill: bool = False,
) -> pd.DataFrame:
    """
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
    return priority_df  # type: ignore


def update_dist(
    dist: dict[str, float], df: pd.DataFrame, wanted_df: pd.DataFrame
) -> tuple[dict[str, float], pd.DataFrame]:
    """
    Updates the distribution of protein quantities based on the given DataFrame and returns the updated distribution
    and the modified wanted DataFrame.

    Args:
        dist (Dict[str, float]): A dictionary where keys are protein names and values are their quantities.
        df (pd.DataFrame): A DataFrame containing the quantities to be subtracted from the distribution.
        wanted_df (pd.DataFrame): A DataFrame from which the rows corresponding to the indices of df will be dropped.

    Returns:
        Tuple[Dict[str, float], pd.DataFrame]: A tuple containing the updated distribution dictionary
        and the modified wanted DataFrame.
    """
    # Initialize an empty dictionary to store the updated distribution
    actual = {}

    # Drop the rows from wanted_df that correspond to the indices in df
    wanted_df = wanted_df.drop(df.index)  # type: ignore

    # Iterate over each protein in the distribution dictionary
    for protein in dist:
        # Calculate the new quantity by subtracting the sum of the quantities
        #  in df from the current quantity in dist
        new_quantity = dist[protein] - df[protein].sum()

        # If the new quantity is zero, skip adding it to the actual dictionary
        if not new_quantity:
            continue

        # Update the actual dictionary with the new quantity
        actual[protein] = new_quantity

    # Return the updated distribution dictionary and the modified wanted DataFrame
    return actual, wanted_df


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
        msg += f"RECIPES WITH COOKING TIMES BETWEEN {cooking_times_not_full} not fulfilled."

    # Return warning message with the constructed message string
    return (1, f"TAXONOMY {taxonomy_id} WARNING! {msg}")


def get_output(
    status: str,
    msg: str,
    ings_out: list[str | int] = [],  # noqa: B006
    prices_out: list[str | int] = [],  # noqa: B006
    cooking_times_out: list[str | int] = [],  # noqa: B006
    recipes: list[dict[str, str]] = [],  # noqa: B006
) -> dict[str, str | int | list[str | int] | list[dict[str, str]]]:
    """
    Constructs a dictionary containing various output details.

    Args:
        status (str): The status of the operation.
        msg (str): A message providing additional information about the status.
        ings_out (List[str], optional): A list of ingredients. Defaults to an empty list.
        prices_out (List[str], optional): A list of price categories. Defaults to an empty list.
        cooking_times_out (List[str], optional): A list of cooking times. Defaults to an empty list.
        recipes (List[Dict[str, str]], optional): A list of recipes. Defaults to an empty list.

    Returns:
        Dict[str, Any]: A dictionary containing the provided details.
    """
    # Construct and return the output dictionary
    return {
        "ingredients": ings_out,
        "price_categories": prices_out,
        "cooking_times": cooking_times_out,
        "STATUS": status,
        "STATUS_MSG": msg,
        "recipes": recipes,
    }


def generate_menu(
    num_recipes: int,
    rules: dict[str, int | list[dict[str, int | str]]],
    df_recipes: pd.DataFrame,
    final_df: pd.DataFrame,
) -> dict[str, int | str | list[str | int] | list[dict[str, str]]]:
    """
    Generate a menu combination from all recipes.

    Args:
        num_recipes (int): The number of recipes to generate.
        rules (Dict[str, Union[int, List[Dict[str, Union[int, str]]]]]): The rules to follow for generating the menu.
        df_recipes (pd.DataFrame): The dataframe of all recipes.
        final_df (pd.DataFrame): The dataframe of previously generated recipes.

    Returns:
        Dict[str, Union[int, str, List[Union[str, int]], List[Dict[str, str]]]]: A dictionary containing the
        number of generated menu recipes, the number of recipes that could not be fulfilled, and messages about
        the generation process.
    """

    taxonomy_id = rules[TAX_COLUMN]
    if len(df_recipes) < 1:
        return get_output(0, f"No recipes with taxonomy {taxonomy_id}.")  # type: ignore
    # Parse input data

    min_average_rating = rules["min_average_rating"]

    ingredients = rules["main_ingredients"] if rules["main_ingredients"] else []

    prices = rules["price_categories"] if rules["price_categories"] else []

    cooking_times = rules["cooking_times"] if rules["cooking_times"] else []

    # PIM_data = PIM_data[~PIM_data[ING_COLUMN].isin(ingredients_to_exclude)]
    # df_after_excluded = exclude_recipes(PIM_data, min_rating, ingredients_to_exclude,
    # min_recipe_cost, max_recipe_cost)

    PIM_data_excluded = exclude_recipes(  # noqa: N806
        df_recipes,
        min_average_rating,  # type: ignore
    )  # Exclude recipes based on rating.

    # Join Coolumns 'CookingTimeTo' and CookingTimeFrom to a single column with the interval
    PIM_data_excluded[COOKING_TIME_COLUMN] = PIM_data_excluded.apply(
        lambda row: f"{row[COOKING_TIME_FROM_COLUMN]}_{row[COOKING_TIME_TO_COLUMN]}",
        axis=1,
    )
    # These lists are used to keep information about constraints not found in the data,
    # are then used to generate the output.
    ings_not_found = []
    prices_not_found = []
    cooking_times_not_found = []
    dist = {}  # Dist dictionary is used to store the constraints found in data.
    # This dict is mutable and will change in the algorithm run.
    for price in prices:  # type: ignore
        if (
            price["price_category_id"]
            in PIM_data_excluded[PRICE_CATEGORY_COLUMN].unique()
        ):
            dist[f"{PRICE_CATEGORY_COLUMN}_{int(price['price_category_id'])}"] = price[
                "quantity"
            ]

        else:
            prices_not_found.append(int(price["price_category_id"]))

    for ingredient in ingredients:  # type: ignore
        if ingredient[ING_COLUMN] in PIM_data_excluded[ING_COLUMN].unique():
            dist[f"{ING_COLUMN}_{int(ingredient[ING_COLUMN])}"] = ingredient["quantity"]
        else:
            ings_not_found.append(int(ingredient[ING_COLUMN]))

    for cooking_time in cooking_times:  # type: ignore
        if (
            f"{cooking_time['time_from']}_{cooking_time['time_to']}"
            in PIM_data_excluded[COOKING_TIME_COLUMN].unique()
        ):
            dist[
                f"{COOKING_TIME_COLUMN}_{cooking_time['time_from']}_{cooking_time['time_to']}"
            ] = cooking_time["quantity"]
        else:
            cooking_times_not_found.append(
                f"{COOKING_TIME_COLUMN}_{cooking_time['time_from']}_{cooking_time['time_to']}"
            )

    df_recipe_group = (
        PIM_data_excluded[
            [RECIPE_ID_COLUMN, RATING_COLUMN, PRICE_CATEGORY_COLUMN, UNIVERSE_COLUMN]
        ]
        .groupby(RECIPE_ID_COLUMN)
        .agg({RATING_COLUMN: "mean", PRICE_CATEGORY_COLUMN: "mean"})
    )

    bool_result = (
        PIM_data_excluded[
            [RECIPE_ID_COLUMN, RATING_COLUMN, PRICE_CATEGORY_COLUMN, UNIVERSE_COLUMN]
        ]
        .groupby(RECIPE_ID_COLUMN)[UNIVERSE_COLUMN]
        .any()
    )
    # Merge the two DataFrames to get the final result
    df_recipe_group[UNIVERSE_COLUMN] = df_recipe_group.index.map(bool_result)  # type: ignore

    out = get_dummies(
        PIM_data_excluded,
        df_recipe_group,  # type: ignore
    )  # Create a dummie df with 0's and 1's.
    available_recipes = out.copy()

    recipes_ordered = get_priorities(
        PIM_data_excluded
    )  # List with recipes ordered by priority.

    mult_df = get_priority_df(
        available_recipes, dist, recipes_ordered
    )  # DF with priority recipes
    mult_df = mult_df[mult_df["n_overlay"] >= 0]
    final_df = pd.DataFrame()  # DF where the recipes to be output are stored.

    # ALGORITHM BEGINS

    while len(final_df) < num_recipes and len(mult_df) > 0:
        row = mult_df.sample(1)  # Select 1 recipe
        final_df = pd.concat([final_df, row])  # Add to final DF #type: ignore
        dist, available_recipes = update_dist(
            dist,
            row,  # type: ignore
            available_recipes,  # type: ignore
        )  # Update distribution with new recipe constraints
        mult_df = get_priority_df(
            available_recipes, dist, recipes_ordered
        )  # Get new prioritized recipes with the new distribution
        mult_df = (
            mult_df[mult_df["n_overlay"] >= 0] if len(mult_df) > 0 else mult_df
        )  # Because of empty DF if 0 recipes are available

        final_df[CONSTRAINT_COLUMN] = True
    # If there are not enough recipes, get random recipes.
    random_recipes = 0
    while len(final_df) < num_recipes and len(available_recipes) > 0:
        remaining_df = get_priority_df(
            available_recipes, dist, recipes_ordered, True
        )  # Even if random recipes, priority is still respected.

        remaining = min(len(remaining_df), num_recipes) - len(final_df)
        remaining_df_add = remaining_df.sample(remaining)
        remaining_df_add[CONSTRAINT_COLUMN] = False

        available_recipes = available_recipes.drop(remaining_df_add.index)  # type: ignore

        random_recipes += remaining

        final_df = pd.concat([final_df, remaining_df_add])

    msg_recipes = (
        f" Number of Recipes needed to fulfill constraints is {len(final_df) - random_recipes}, adding {random_recipes} random recipes."  # noqa: E501
        if random_recipes < len(final_df)
        else f" Number of Recipes needed to fulfill constraints is {random_recipes}, adding {random_recipes} random recipes."  # noqa: E501
        if random_recipes == len(final_df)
        else ""
    )

    ings_out, ings_not_full = get_ingredient_distribution(
        ingredients,  # type: ignore
        final_df,
        ings_not_found,  # type: ignore
    )

    prices_out, prices_not_full = get_prices_distribution(
        prices,  # type: ignore
        final_df,
        prices_not_found,  # type: ignore
    )

    cooking_times_out, cooking_times_not_full = get_cooking_times_distribution(
        cooking_times,  # type: ignore
        final_df,
        cooking_times_not_found,  # pyright: ignore
    )

    status, msg_data = get_message(
        ings_not_full,  # type: ignore # pyright: ignore
        prices_not_full,  # type: ignore # pyright: ignore
        cooking_times_not_full,
        ings_not_found,
        prices_not_found,
        cooking_times_not_found,
        bool(num_recipes - len(final_df)),
        taxonomy_id,  # type: ignore
    )

    msg = msg_data + msg_recipes
    final_recipes = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(final_df.index)][  # pyright: ignore
        [RECIPE_ID_COLUMN, ING_COLUMN]
    ].drop_duplicates()  # pyright: ignore

    final_recipes = final_recipes.merge(
        final_df[[CONSTRAINT_COLUMN]].reset_index(), on=RECIPE_ID_COLUMN, how="inner"
    )

    recipes = final_recipes.to_dict(orient="records")

    return get_output(status, msg, ings_out, prices_out, cooking_times_out, recipes)  # type: ignore


# def generate_menu_companies(week, year, companies, local: bool = False, datalake_handler: BlobConnector = None):
#    output = {"year": year, "week": week, "companies": []}
#    messages = []
#    status = []
#    for company in companies:
#        company_id = company["company_id"]
#        output_company = {"company_id": company_id, "taxonomies": []}
#        messages.append(f"COMPANY {company_id}:")
#        available_recipes = list(map(int, company["available_recipes"]))
#        required_recipes = list(map(int, company["required_recipes"]))
#
#        # PIM DATA
#
#        df_recipes = get_PIM_data(
#            company_id=company_id,
#            local=local,
#            datalake_handler=datalake_handler,
#        )
#
#        df_recipes = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(available_recipes)]
#
#        df_recipes = postprocess_recipe_data(df_recipes=df_recipes)
#
#        final_df = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(required_recipes)]
#        final_df = final_df[[RECIPE_ID_COLUMN, ING_COLUMN]].drop_duplicates()
#        final_df[CONSTRAINT_COLUMN] = True
#
#        for taxonomy in company["taxonomies"]:
#            num_recipes = taxonomy["quantity"]
#
#            df_taxonomies = df_recipes[df_recipes[TAX_COLUMN] == taxonomy[TAX_COLUMN]]
#
#            output_tax = generate_menu(num_recipes, taxonomy, df_taxonomies, final_df)
#
#            final_tax = pd.DataFrame(
#                output_tax["recipes"],
#                columns=[RECIPE_ID_COLUMN, ING_COLUMN, CONSTRAINT_COLUMN],
#            )
#
#            final_df = pd.concat([final_df, final_tax])
#            df_recipes = df_recipes[~df_recipes[RECIPE_ID_COLUMN].isin(final_tax[RECIPE_ID_COLUMN])]
#
#            messages.append(output_tax["STATUS_MSG"])
#            status.append(output_tax["STATUS"])
#
#            output_company["taxonomies"].append(
#                {
#                    "taxonomy_id": taxonomy[TAX_COLUMN],
#                    "wanted": taxonomy["quantity"],
#                    "actual": len(final_tax),
#                    "main_ingredients": output_tax["ingredients"],
#                    "price_categories": output_tax["price_categories"],
#                    "cooking_times": output_tax["cooking_times"],
#                }
#            )
#
#        if len(final_df) < company["num_recipes"] and len(df_recipes):
#            remaining = min(len(df_recipes), company["num_recipes"] - len(final_df))
#            fill_df = df_recipes[[RECIPE_ID_COLUMN, ING_COLUMN]].drop_duplicates().sample(remaining)
#            fill_df[CONSTRAINT_COLUMN] = False
#            final_df = pd.concat([final_df, fill_df])
#        output_company["recipes"] = final_df.to_dict(orient="records")
#        output["companies"].append(output_company)
#    output["STATUS"] = max(status)
#    output["STATUS_MSG"] = " ".join(messages)
#
#    return output


def generate_menu_companies_sous_chef(
    week: int,
    year: int,
    companies: list[dict[str, Any]],
    input_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Generate menu for companies based on available and required recipes.

    Args:
        week (int): The week number for which the menu is being generated.
        year (int): The year for which the menu is being generated.
        companies (List[Dict[str, Any]]): List of companies with their respective details.
        recipe_bank: (Optional[pd.DataFrame]): DataFrame containing recipe data.

    Returns:
        Dict[str, Any]: A dictionary containing the generated menu and status messages.
    """
    output = {"year": year, "week": week, "companies": []}
    messages = []
    status = []

    for company in companies:
        company_id = company["company_id"]
        output_company = {"company_id": company_id, "taxonomies": []}
        messages.append(f"COMPANY {company_id}:")

        # Convert available and required recipes to list of integers
        available_recipes = list(map(int, company["available_recipes"]))
        required_recipes = list(map(int, company["required_recipes"]))

        # Filter recipes based on available recipes
        df_recipes = input_df
        df_recipes = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(available_recipes)]  # type: ignore # pyright: ignore

        # Post-process the recipe data
        df_recipes = postprocess_recipe_data(df_recipes=df_recipes)  # pyright: ignore

        # Filter recipes based on required recipes
        final_df = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(required_recipes)]
        final_df = final_df[[RECIPE_ID_COLUMN, ING_COLUMN]].drop_duplicates()  # pyright: ignore
        final_df[CONSTRAINT_COLUMN] = True

        for taxonomy in company["taxonomies"]:
            num_recipes = taxonomy["quantity"]

            # Filter recipes based on taxonomy
            df_taxonomies = df_recipes[df_recipes[TAX_COLUMN] == taxonomy[TAX_COLUMN]]

            # Generate menu for the taxonomy
            output_tax = generate_menu(num_recipes, taxonomy, df_taxonomies, final_df)  # pyright: ignore

            final_tax = pd.DataFrame(
                output_tax["recipes"],  # type: ignore
                columns=[RECIPE_ID_COLUMN, ING_COLUMN, CONSTRAINT_COLUMN],  # pyright: ignore
            )

            # Concatenate the final taxonomy recipes to the final DataFrame
            final_df = pd.concat([final_df, final_tax])
            df_recipes = df_recipes[
                ~df_recipes[RECIPE_ID_COLUMN].isin(final_tax[RECIPE_ID_COLUMN])  # pyright: ignore
            ]

            messages.append(output_tax["STATUS_MSG"])
            status.append(output_tax["STATUS"])

            output_company["taxonomies"].append(
                {
                    "taxonomy_id": taxonomy[TAX_COLUMN],
                    "wanted": taxonomy["quantity"],
                    "actual": len(final_tax),
                    "main_ingredients": output_tax["ingredients"],
                    "price_categories": output_tax["price_categories"],
                    "cooking_times": output_tax["cooking_times"],
                }
            )

        # Fill remaining recipes if needed
        if len(final_df) < company["num_recipes"] and len(df_recipes):
            remaining = min(len(df_recipes), company["num_recipes"] - len(final_df))
            fill_df = (
                df_recipes[[RECIPE_ID_COLUMN, ING_COLUMN]]
                .drop_duplicates()  # pyright: ignore
                .sample(remaining)
            )
            fill_df[CONSTRAINT_COLUMN] = False
            final_df = pd.concat([final_df, fill_df])

        output_company["recipes"] = final_df.to_dict(orient="records")  # pyright: ignore
        output["companies"].append(output_company)

    output["STATUS"] = max(status)
    output["STATUS_MSG"] = " ".join(messages)

    return output


async def generate_menu_companies_api(
    week: int, year: int, companies: list[dict[str, Any]]
) -> dict[str, Any]:
    """
    Generate menu for companies based on available and required recipes.

    Args:
        week (int): The week number for which the menu is being generated.
        year (int): The year for which the menu is being generated.
        companies (List[Dict[str, Any]]): List of companies with their respective details.
        recipe_bank: (Optional[pd.DataFrame]): DataFrame containing recipe data.

    Returns:
        Dict[str, Any]: A dictionary containing the generated menu and status messages.
    """
    output = {"year": year, "week": week, "companies": []}
    messages = []
    status = []

    for company in companies:
        company_id = company["company_id"]
        output_company = {"company_id": company_id, "taxonomies": []}
        messages.append(f"COMPANY {company_id}:")

        # Convert available and required recipes to list of integers
        available_recipes = list(map(int, company["available_recipes"]))
        required_recipes = list(map(int, company["required_recipes"]))
        # Initialize count of number of recipes requested
        count_recipes_requested = len(required_recipes)
        count_taxonomies_requested = 0

        # Filter recipes based on available recipes

        # df_recipes = await (
        #    data_science_data_lake.directory("test-folder/MP20/")
        #    .parquet_at("PIM_RecipeBank_LMK_PRD")
        #    .to_pandas()  # we need pandas as the rest of the processing uses pandas methods like isin etc
        # )  # gives error coroutine object is not subscriptable in the next line
        # df_recipes = pd.read_csv("./data_api/input_recipes.csv")
        df_recipes = await input_recipes.input_recipes(company_id=company_id)
        original_columns = df_recipes.columns
        df_recipes.columns = pd.Index([camel_to_snake(x) for x in original_columns])

        df_recipes = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(available_recipes)]

        # Post-process the recipe data
        df_recipes = postprocess_recipe_data(df_recipes=df_recipes)  # pyright: ignore

        # Filter recipes based on required recipes
        final_df = df_recipes[df_recipes[RECIPE_ID_COLUMN].isin(required_recipes)]
        final_df = final_df[[RECIPE_ID_COLUMN, ING_COLUMN]].drop_duplicates()  # pyright: ignore
        final_df[CONSTRAINT_COLUMN] = True

        for taxonomy in company["taxonomies"]:
            num_recipes = taxonomy["quantity"]
            count_taxonomies_requested += num_recipes
            count_recipes_requested += num_recipes

            # Filter recipes based on taxonomy
            df_taxonomies = df_recipes[df_recipes[TAX_COLUMN] == taxonomy[TAX_COLUMN]]

            # Generate menu for the taxonomy
            output_tax = generate_menu(num_recipes, taxonomy, df_taxonomies, final_df)  # pyright: ignore

            final_tax = pd.DataFrame(
                output_tax["recipes"],  # type: ignore
                columns=[RECIPE_ID_COLUMN, ING_COLUMN, CONSTRAINT_COLUMN],  # pyright: ignore
            )

            # Concatenate the final taxonomy recipes to the final DataFrame
            final_df = pd.concat([final_df, final_tax])
            df_recipes = df_recipes[
                ~df_recipes[RECIPE_ID_COLUMN].isin(final_tax[RECIPE_ID_COLUMN])  # pyright: ignore
            ]

            messages.append(output_tax["STATUS_MSG"])
            status.append(output_tax["STATUS"])

            output_company["taxonomies"].append(
                {
                    "taxonomy_id": taxonomy[TAX_COLUMN],
                    "wanted": taxonomy["quantity"],
                    "actual": len(final_tax),
                    "main_ingredients": output_tax["ingredients"],
                    "price_categories": output_tax["price_categories"],
                    "cooking_times": output_tax["cooking_times"],
                }
            )
        # Remove duplicate recipe_id
        final_df = final_df.drop_duplicates(subset=RECIPE_ID_COLUMN, keep="first")  # pyright: ignore

        # Fill remaining recipes if needed
        if len(final_df) < count_recipes_requested and len(df_recipes):
            remaining = min(len(df_recipes), count_recipes_requested) - len(final_df)
            fill_df = (
                df_recipes[[RECIPE_ID_COLUMN, ING_COLUMN]]
                .drop_duplicates()  # pyright: ignore
                .sample(remaining)
            )
            fill_df[CONSTRAINT_COLUMN] = False
            final_df = pd.concat([final_df, fill_df])
            messages.append(
                f"Duplicates found. Replacing them with {len(fill_df)} random recipes"
            )

        output_company["recipes"] = final_df.to_dict(orient="records")  # pyright: ignore
        output["companies"].append(output_company)

    output["STATUS"] = max(status)
    output["STATUS_MSG"] = " ".join(messages)

    return output
