import logging

import pandas as pd
from menu_optimiser.common import (
    Args,
    RequestMenu,
    RequestTaxonomy,
    ResponseMenu,
    ResponseAverageRating,
    ResponseTaxonomy,
    ResponseMainIngredient,
    ResponsePriceCategory,
    ResponseCookingTime,
    ReponseRecipe,
    ResponseCompany,
)
from menu_optimiser.data import (
    preprocess_recipe_data,
    get_recipe_bank_data,
)

from menu_optimiser.helpers import (
    exclude_recipes,
    get_dummies,
    get_distribution,
    get_priority_df,
    update_dist,
    get_message,
)


logger = logging.getLogger(__name__)


def generate_menu(
    num_recipes: int,
    rules: RequestTaxonomy,
    df_recipes: pd.DataFrame,
    final_df: pd.DataFrame,
) -> dict[str, str | int | list[str | int] | list[dict[str, str]]]:
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

    taxonomy_id = rules.taxonomy_id
    if len(df_recipes) < 1:
        return {"status": 0, "status_msg": f"No recipes with taxonomy {taxonomy_id}."}
    # Parse input data
    min_average_rating = int(rules.min_average_rating)
    ingredients = rules.main_ingredients if rules.main_ingredients else []
    prices = rules.price_categories if rules.price_categories else []
    cooking_times = rules.cooking_times if rules.cooking_times else []

    PIM_data_excluded = exclude_recipes(
        df_recipes,
        min_average_rating,
    )  # Exclude recipes based on rating.

    # These lists are used to keep information about constraints not found in the data,
    # are then used to generate the output.
    ings_not_found = []
    prices_not_found = []
    cooking_times_not_found = []
    dist = {}  # Dist dictionary is used to store the constraints found in data.
    # This dict is mutable and will change in the algorithm run.
    for price in prices:
        if price.price_category_id in PIM_data_excluded["price_category_id"].unique():
            # TODO: remove conversion to int
            dist[f"price_category_id_{price.price_category_id}"] = price.quantity
        else:
            prices_not_found.append(price.price_category_id)
    # TODO: remove conversion to int
    for ingredient in ingredients:
        if (
            ingredient.main_ingredient_id
            in PIM_data_excluded["main_ingredient_id"].unique()
        ):
            dist[f"main_ingredient_id_{ingredient.main_ingredient_id}"] = (
                ingredient.quantity
            )
        else:
            ings_not_found.append(ingredient.main_ingredient_id)
    # TODO: need to fix this
    for cooking_time in cooking_times:
        if (
            cooking_time.cooking_time_to
            in PIM_data_excluded["cooking_time_to"].unique()
        ):
            dist[f"cooking_time_to_{cooking_time.cooking_time_to}"] = (
                cooking_time.quantity
            )
        else:
            cooking_times_not_found.append(cooking_time.cooking_time_to)

    available_recipes = get_dummies(
        PIM_data_excluded,
    )  # Create a dummie df with 0's and 1's.

    # set index to recipe_id
    available_recipes = available_recipes.set_index("recipe_id")

    # TODO: remove, not needed if everything is universe
    recipes_ordered = [list(PIM_data_excluded["recipe_id"].unique()), []]

    mult_df = get_priority_df(
        available_recipes, dist, recipes_ordered
    )  # DF with priority recipes

    mult_df = mult_df[mult_df["n_overlay"] >= 0]

    final_df = pd.DataFrame()  # DF where the recipes to be output are stored.
    # ALGORITHM BEGINS
    while len(final_df) < num_recipes and len(mult_df) > 0:
        sample = pd.DataFrame(mult_df.sample(1))  # Select 1 recipe
        final_df = pd.concat([final_df, sample])  # Add to final DF #type: ignore
        dist, available_recipes = update_dist(
            dist,
            sample,
            available_recipes,
        )  # Update distribution with new recipe constraints
        mult_df = get_priority_df(
            available_recipes, dist, recipes_ordered
        )  # Get new prioritized recipes with the new distribution
        mult_df = (
            mult_df[mult_df["n_overlay"] >= 0] if len(mult_df) > 0 else mult_df
        )  # Because of empty DF if 0 recipes are available

        final_df["is_constraint"] = True
    # If there are not enough recipes, get random recipes.
    random_recipes = 0
    # TODO: this does not have to be a while loop? Should change to if statement?
    while len(final_df) < num_recipes and len(available_recipes) > 0:
        remaining_df = get_priority_df(
            available_recipes, dist, recipes_ordered, True
        )  # Even if random recipes, priority is still respected

        remaining = min(len(remaining_df), num_recipes - len(final_df))

        remaining_df_add = remaining_df.sample(remaining)
        remaining_df_add["is_constraint"] = False

        available_recipes = available_recipes.drop(list(remaining_df_add.index))

        random_recipes += remaining

        final_df = pd.concat([final_df, remaining_df_add])

    msg_recipes = (
        f" Number of Recipes needed to fulfill constraints is {len(final_df) - random_recipes}, adding {random_recipes} random recipes."
        if random_recipes < len(final_df)
        else f" Number of Recipes needed to fulfill constraints is {random_recipes}, adding {random_recipes} random recipes."
        if random_recipes == len(final_df)
        else ""
    )

    ings_out, ings_not_full = get_distribution(
        items=ingredients,
        df=final_df,
        not_found=ings_not_found,
        field_name="main_ingredient_id",
    )

    prices_out, prices_not_full = get_distribution(
        items=prices,
        df=final_df,
        not_found=prices_not_found,
        field_name="price_category_id",
    )

    cooking_times_out, cooking_times_not_full = get_distribution(
        items=cooking_times,
        df=final_df,
        not_found=cooking_times_not_found,
        field_name="cooking_time_to",
    )

    for ct in cooking_times_out:
        ct["cooking_time_from"] = next(
            (
                ct_in.cooking_time_from
                for ct_in in cooking_times
                if ct_in.cooking_time_to == ct["cooking_time_to"]
            ),
            None,
        )

    status, msg_data = get_message(
        ings_not_full,
        prices_not_full,
        cooking_times_not_full,
        ings_not_found,
        prices_not_found,
        cooking_times_not_found,
        bool(num_recipes - len(final_df)),
        taxonomy_id,
    )

    msg = msg_data + msg_recipes

    final_recipes = pd.DataFrame(
        df_recipes[df_recipes["recipe_id"].isin(list(final_df.index))][
            [
                "recipe_id",
                "main_ingredient_id",
            ]
        ]
    ).drop_duplicates()

    final_recipes = final_recipes.merge(
        final_df[["is_constraint"]].reset_index(),
        on="recipe_id",
        how="inner",
    )

    recipes = final_recipes.to_dict(
        orient="records"
    )  # TODO: simplify after figuering out how to handle is_constraint true/false for random

    output = {
        "ingredients": ings_out,
        "price_categories": prices_out,
        "cooking_times": cooking_times_out,
        "status": status,
        "status_msg": msg,
        "recipes": recipes,  # is constraint and main_ingredient_id is stored here with recipe_id, why?
    }

    return output


async def generate_menu_companies(
    payload: RequestMenu,
) -> ResponseMenu:
    """
    Generate menu for companies based on available and required recipes.

    Parameters
    ----------
    payload : RequestMenu
        The menu request containing week, year, and company details including:
        - week: Week number for menu generation
        - year: Year for menu generation
        - companies: List of companies with their:
            - company_id: Company identifier
            - available_recipes: List of available recipe IDs
            - required_recipes: List of required recipe IDs
            - taxonomies: List of taxonomy requirements with quantities

    Returns
    -------
    ResponseMenu: The generated menu response
    """

    response_companies = []  # TODO: remove
    messages = []  # TODO: remove
    status = []  # TODO: remove

    company = payload.companies[0]
    args = Args(company_id=company.company_id, env="prod")  # type: ignore

    df_recipe_bank = await get_recipe_bank_data(args.company_code, args.recipe_bank_env)
    df_recipes = pd.DataFrame(
        preprocess_recipe_data(df_recipe_bank, only_universe=True)
    )

    messages.append(f"COMPANY {args.company_id}:")

    # Convert available and required recipes to list of integers and remove dupes
    available_recipes = list(dict.fromkeys(map(int, company.available_recipes)))
    required_recipes = list(dict.fromkeys(map(int, company.required_recipes)))

    # Fill final output with required recipes
    final_df = pd.DataFrame(df_recipes[df_recipes["recipe_id"].isin(required_recipes)])
    final_df["is_constraint"] = True

    # Filter recipes based on available recipes
    df_recipes = pd.DataFrame(
        df_recipes[~df_recipes["recipe_id"].isin(required_recipes)]
    )
    df_recipes = pd.DataFrame(
        df_recipes[df_recipes["recipe_id"].isin(available_recipes)]
    )

    total_recipes = len(required_recipes)
    response_taxonomies = []
    for taxonomy in company.taxonomies:
        num_recipes = taxonomy.quantity
        total_recipes += num_recipes
        # Filter recipes based on taxonomy
        df_taxonomies = pd.DataFrame(
            df_recipes[
                df_recipes["taxonomies"].apply(lambda x: taxonomy.taxonomy_id in x)
            ]
        )
        # Generate menu for the taxonomy
        output_tax = generate_menu(num_recipes, taxonomy, df_taxonomies, final_df)

        # if output tax does not contain recipes, create empty dict
        if "recipes" not in output_tax:
            messages.append(output_tax["status_msg"])
            status.append(output_tax["status"])
            continue

        final_tax = pd.DataFrame(output_tax["recipes"])
        final_tax = final_tax[["recipe_id", "is_constraint"]]
        final_tax = pd.merge(df_recipes, final_tax, how="inner", on="recipe_id")

        # Concatenate the final taxonomy recipes to the final DataFrame
        final_df = pd.concat([final_df, final_tax])

        df_recipes = pd.DataFrame(
            df_recipes[~df_recipes["recipe_id"].isin(final_tax["recipe_id"])]
        )

        messages.append(output_tax["status_msg"])
        status.append(output_tax["status"])

        response_taxonomies.append(
            ResponseTaxonomy(
                taxonomy_id=taxonomy.taxonomy_id,
                wanted=taxonomy.quantity,
                actual=len(final_tax),
                main_ingredients=[
                    ResponseMainIngredient(**ing)
                    for ing in output_tax["ingredients"]  # type: ignore
                ],
                price_categories=[
                    ResponsePriceCategory(**pc)
                    for pc in output_tax["price_categories"]  # type: ignore
                ],
                cooking_times=[
                    ResponseCookingTime(**ct)
                    for ct in output_tax["cooking_times"]  # type: ignore
                ],
                average_rating=ResponseAverageRating(
                    average_rating=taxonomy.min_average_rating,
                    wanted=taxonomy.quantity,
                    actual=len(final_tax),
                ),
            )
        )

    # Fill remaining recipes if needed
    if len(final_df) < total_recipes and len(df_recipes):
        remaining = min(len(df_recipes), total_recipes - len(final_df))

        fill_df = pd.DataFrame(df_recipes.sample(remaining))
        fill_df["is_constraint"] = False

        final_df = pd.concat([final_df, fill_df])

    final_df = pd.DataFrame(final_df)

    recipes = [ReponseRecipe(**row) for row in final_df.to_dict(orient="records")]

    response_company = ResponseCompany(
        company_id=args.company_id,
        recipes=recipes,
        taxonomies=response_taxonomies,
    )

    response_companies.append(response_company)

    return ResponseMenu(
        week=payload.week,
        year=payload.year,
        companies=response_companies,
        status=max(status),
        status_msg=" ".join(messages),
    )
