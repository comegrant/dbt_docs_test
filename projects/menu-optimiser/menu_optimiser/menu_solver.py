import pulp
import pandas as pd

from menu_optimiser.data import create_dataset
from menu_optimiser.common import (
    Args,
    BusinessRule,
    RequestMenu,
    ResponseMenu,
    ResponseCompany,
    ResponseTaxonomy,
    ReponseRecipe,
    ResponseMainIngredient,
    ResponsePriceCategory,
    ResponseCookingTime,
    ResponseAverageRating,
)

from collections import defaultdict
from typing import Any
from menu_optimiser.config import CompanyOptimizationConfig, OptimizationConfig
from menu_optimiser.db import get_recipe_ingredients_data
from menu_optimiser.analysis import adjust_available_recipes


# TODO: find sweet spot for time limit, max attempt, and +- storage and recipes
# TODO: must also have a minimum for amount of recipes?? cannot do == anymore


class Recipes:
    """
    A class to manage and query recipe data for menu optimization.

    Attributes:
        df (pd.DataFrame): The DataFrame containing recipe data.
        recipe_index (dict): Maps recipe_id to recipe data as a dictionary.
        taxonomy_to_recipes (dict): Maps taxonomy IDs to lists of recipe IDs belonging to each taxonomy.

    Methods:
        get_recipe(recipe_id): Retrieve the recipe data for a given recipe_id.
        get_recipe_ids(): Return a list of all recipe IDs.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.recipe_index = self._build_index()
        self.taxonomy_to_recipes = self._build_taxonomy_index()

    def _build_index(self):
        return self.df.set_index("recipe_id").to_dict("index")

    def _build_taxonomy_index(self):
        """Compute which recipes belong to each taxonomy."""
        taxonomy_map = defaultdict(set)
        for recipe_id, recipe_data in self.recipe_index.items():
            taxonomies = recipe_data.get("taxonomies", [])
            for taxonomy in taxonomies:
                taxonomy_map[taxonomy].add(recipe_id)
        return {k: list(v) for k, v in taxonomy_map.items()}

    def get_recipe(self, recipe_id: int) -> dict[str, Any]:
        return self.recipe_index.get(recipe_id, {})

    def get_recipe_ids(self) -> list[int]:
        return list(self.df["recipe_id"])


def validate_business_rules(
    business_rules: dict[int, BusinessRule],
) -> tuple[dict[int, BusinessRule], list[str]]:
    """
    Validate and adjust business rules to ensure that the sum of requested main ingredients,
    price categories, and cooking times does not exceed the total number of recipes requested
    for each taxonomy. If any category exceeds the total, the largest value in that category
    is reduced to match the total, and an error message is recorded.

    Args:
        business_rules (dict[int, BusinessRule]): A mapping of taxonomy IDs to BusinessRules.

    Returns:
        tuple[dict[int, BusinessRule], list[str]]:
            - The possibly adjusted business_rules.
            - A list of error messages describing any adjustments made.
    """
    error_msg = []
    for key, value in business_rules.items():
        total = value.total
        total_main_ingredients = sum(value.main_ingredients.values())
        total_price_category = sum(value.price_category.values())
        total_cooking_time = sum(value.cooking_time_to.values())

        if total < total_main_ingredients:
            difference = total_main_ingredients - total
            biggest_key = max(value.main_ingredients, key=value.main_ingredients.get)  # type: ignore
            value.main_ingredients[biggest_key] -= difference
            error_msg.append(
                f"Adjusted requested main ingredients amount from {total_main_ingredients} to {sum(value.main_ingredients.values())} for taxonomy {key}."
            )

        if total < total_price_category:
            difference = total_price_category - total
            biggest_key = max(value.price_category, key=value.price_category.get)  # type: ignore
            value.price_category[biggest_key] -= difference
            error_msg.append(
                f"Adjusted requested price category amount from {total_price_category} to {sum(value.price_category.values())} for taxonomy {key}."
            )

        if total < total_cooking_time:
            difference = total_cooking_time - total
            biggest_key = max(value.cooking_time_to, key=value.cooking_time_to.get)  # type: ignore
            value.cooking_time_to[biggest_key] -= difference
            error_msg.append(
                f"Adjusted requested cooking time amount from {total_cooking_time} to {sum(value.cooking_time_to.values())} for taxonomy {key}."
            )

    return business_rules, error_msg


def extract_taxonomy_constraints(
    request_menu: RequestMenu,
) -> tuple[dict[int, BusinessRule], int, str]:
    """
    Extract taxonomy constraints from a RequestMenu and build a mapping of taxonomy IDs to BusinessRule objects.

    Args:
        request_menu (RequestMenu): The menu request containing taxonomy requirements.

    Returns:
        tuple:
            - dict[int, BusinessRule]: Mapping from taxonomy ID to BusinessRule specifying constraints for that taxonomy.
            - int: Total number of recipes requested across all taxonomies.
            - str: Error messages (if any) generated during validation of business rules.
    """
    taxonomy_requests = {}

    total_recipes = 0
    for company in request_menu.companies:
        for taxonomy in company.taxonomies:
            taxonomy_requests[taxonomy.taxonomy_id] = BusinessRule(
                total=taxonomy.quantity,
                main_ingredients={
                    mi.main_ingredient_id: mi.quantity
                    for mi in taxonomy.main_ingredients
                },
                price_category={
                    pc.price_category_id: pc.quantity
                    for pc in taxonomy.price_categories
                },
                cooking_time_to={
                    ct.cooking_time_to: ct.quantity for ct in taxonomy.cooking_times
                },
            )
            total_recipes += taxonomy.quantity

    taxonomy_requests, error_msg = validate_business_rules(taxonomy_requests)

    return taxonomy_requests, total_recipes, ". ".join(error_msg)


def _adjust_storage_parameters(
    attempt: int,
    max_attempts: int,
    current_cold: int,
    current_dry: int,
    original_cold: int,
    original_dry: int,
    removed_recipes: int,
) -> tuple[int, int, int]:
    """
    Adjust storage parameters (cold and dry storage) or the number of required recipes
    during iterative optimization attempts.

    Returns:
        tuple[int, int, int]: Updated (cold_storage, dry_storage, removed_recipes).
            - If the number of attempts exceeds half of max_attempts or cold storage is depleted,
              increment removed_recipes and reset storage to original values.
            - Otherwise, decrease cold storage by 1 and increase dry storage by 1.
    """
    if attempt > max_attempts // 2 or current_cold < 1:
        # FALLBACK
        removed_recipes += 1
        return original_cold, original_dry, removed_recipes
    else:
        return current_cold - 1, current_dry + 1, removed_recipes


async def solve_menu_and_storage(
    available_recipes_df: pd.DataFrame,
    required_recipes_df: pd.DataFrame,
    ingredients_df: pd.DataFrame,
    taxonomy_constraints: dict[int, BusinessRule],
    total_required_recipes: int,
    company_config: OptimizationConfig,
    linked_recipes: None | dict[str, list[int]] = None,
    max_attempts: int = 20,  # TODO: move to config?
) -> tuple[pulp.LpProblem, str]:  # pyright: ignore
    """
    Iteratively solve the menu optimization problem while adjusting storage and recipe constraints.

    This function attempts to find a feasible menu selection that satisfies business rules,
    storage constraints (cold and dry), and recipe requirements. If no solution is found,
    it progressively relaxes storage constraints or reduces the number of required recipes
    up to a maximum number of attempts.

    Args:
        available_recipes_df (pd.DataFrame): DataFrame of available recipes to select from.
        required_recipes_df (pd.DataFrame): DataFrame of recipes that must be included.
        ingredients_df (pd.DataFrame): DataFrame mapping recipes to their ingredients and storage types.
        taxonomy_constraints (dict[int, BusinessRule]): Mapping of taxonomy IDs to business rules.
        total_required_recipes (int): Total number of recipes required (excluding required recipes).
        company_config (OptimizationConfig): Company-specific configuration.
        linked_recipes (None | dict[str, list[int]], optional): Optional mapping for linking recipes.
        max_attempts (int, optional): Maximum number of attempts to find a feasible solution.

    Returns:
        tuple[pulp.LpProblem, str]:
            - The solved PuLP problem (NB: even if infeasible!).
            - A string containing error or status messages.
    """
    original_cold_storage = company_config.max_cold_storage
    original_dry_storage = company_config.max_dry_storage

    max_cold_storage = original_cold_storage
    max_dry_storage = original_dry_storage
    removed_recipes = 0

    initial_error_msg = ""
    if company_config.amount_of_recipes is None:
        amount_of_recipes = total_required_recipes
    else:
        if company_config.amount_of_recipes < total_required_recipes:
            initial_error_msg = f"Requested recipes ({company_config.amount_of_recipes}) is less than required recipes ({total_required_recipes}). "
            amount_of_recipes = total_required_recipes
        else:
            amount_of_recipes = company_config.amount_of_recipes

    for attempt in range(max_attempts + 1):
        problem, error_messages = await solve_menu(
            available_recipes_df,
            required_recipes_df,
            ingredients_df,
            taxonomy_constraints,
            int(amount_of_recipes - removed_recipes),
            company_config,
            max_cold_storage,
            max_dry_storage,
            linked_recipes=linked_recipes if attempt < max_attempts / 2 else None,
        )

        if problem.status == 1:  # 1 == OPTIMAL
            if attempt != 0 and removed_recipes == 0:
                error_messages += (
                    "Adjusted storage: cold: "
                    + str(max_cold_storage)
                    + ", dry: "
                    + str(max_dry_storage)
                )
            if removed_recipes > 0:
                error_messages += (
                    "Removed "
                    + str(removed_recipes)
                    + " recipe(s) to fit storage constraints."
                )
            return problem, initial_error_msg + error_messages

        if attempt == max_attempts:
            error_messages = "NO SOLUTION FOUND. " + error_messages
            return problem, initial_error_msg + error_messages

        max_cold_storage, max_dry_storage, removed_recipes = _adjust_storage_parameters(
            attempt,
            max_attempts,
            max_cold_storage,
            max_dry_storage,
            original_cold_storage,
            original_dry_storage,
            removed_recipes,
        )


async def solve_menu(
    available_recipes_df: pd.DataFrame,
    required_recipes_df: pd.DataFrame,
    ingredients_df: pd.DataFrame,
    taxonomy_constraints: dict[int, BusinessRule],
    total_required_recipes: int,
    company_config: OptimizationConfig,
    max_cold_storage: int,
    max_dry_storage: int,
    linked_recipes: None | dict[str, list[int]] = None,
) -> tuple[pulp.LpProblem, str]:
    """
    Solve the menu selection problem using PuLP.

    Args:
        available_recipes_df (pd.DataFrame): DataFrame of available recipes to select from.
        required_recipes_df (pd.DataFrame): DataFrame of recipes that must be included.
        ingredients_df (pd.DataFrame): DataFrame mapping recipes to their ingredients and storage types.
        taxonomy_constraints (dict[int, BusinessRule]): Mapping of taxonomy IDs to their business rules.
        total_required_recipes (int): Total number of recipes required (excluding required recipes).
        company_config (OptimizationConfig): Company-specific configuration.
        max_cold_storage (int): Maximum number of cold storage ingredients allowed.
        max_dry_storage (int): Maximum number of dry storage ingredients allowed.
        linked_recipes (None | dict[str, list[int]], optional): Optional mapping for linking recipes.

    Returns:
        tuple[pulp.LpProblem, str]: The solved PuLP problem and a string containing any error or status messages.
    """

    required_recipes = Recipes(required_recipes_df)
    recipes = Recipes(pd.concat([available_recipes_df, required_recipes_df]))
    error_messages = []
    problem = pulp.LpProblem("Menu_Selection", pulp.LpMaximize)

    required_recipe_ids = required_recipes.get_recipe_ids()
    recipe_ids = recipes.get_recipe_ids()

    total_required_recipes += len(required_recipe_ids)

    diversity_config = company_config.diversity_config

    # CREATE TAXONOMY-RECIPE COMBO VARIABLES
    x = {
        rid: {
            tax: pulp.LpVariable(f"x_{rid}_{tax}", cat="Binary")
            for tax in taxonomy_constraints
            if tax in recipes.recipe_index[rid]["taxonomies"]
        }
        for rid in recipe_ids
    }

    # CREATE GLOBAL RECIPE VARIABLES
    z = {rid: pulp.LpVariable(f"z_{rid}", cat="Binary") for rid in recipe_ids}

    # LINK TAXONOMY-RECIPE COMBO VARIABLES TO GLOBAL RECIPE VARIABLES
    for rid in x:
        for tax in x[rid]:
            problem += x[rid][tax] <= z[rid]

    # FORCE SELECTION OF REQUIRED RECIPES
    for rrid in required_recipe_ids:
        if rrid in z:
            problem += z[rrid] == 1
        else:
            error_messages.append(f"Required recipe {rrid} not found")

    # -------------------------
    # INGREDIENT VARIABLES
    # -------------------------
    unique_ingredients = ingredients_df[
        ["ingredient_id", "is_cold_storage"]
    ].drop_duplicates()

    ingredient_vars = {}
    ingredient_recipe_map = {}

    # CREATE INGREDIENT VARIABLES
    for _, row in unique_ingredients.iterrows():
        ingredient_id = row["ingredient_id"]
        ingredient_vars[ingredient_id] = pulp.LpVariable(
            f"ing_{ingredient_id}", cat="Binary"
        )

        # map ingredient to recipes
        recipes_using_ingredient = pd.unique(
            ingredients_df[ingredients_df["ingredient_id"] == ingredient_id][
                "recipe_id"
            ]
        )

        valid_recipes = [rid for rid in recipes_using_ingredient if rid in z]
        ingredient_recipe_map[ingredient_id] = valid_recipes

    # LINK INGREDIENT VARIABLES TO GLOBAL RECIPE VARIABLES
    for ingredient_id, valid_recipes in ingredient_recipe_map.items():
        if valid_recipes:
            # ingredient is activated if ANY recipe using it is selected
            # and 0 if NO recipe using it is selected
            problem += ingredient_vars[ingredient_id] <= pulp.lpSum(
                [z[rid] for rid in valid_recipes]
            )
            # ensure ingredient is activated if any recipe using it is selected
            # if any recipe is selected, ingredient must be 1
            for rid in valid_recipes:
                problem += ingredient_vars[ingredient_id] >= z[rid]

    # -------------------------
    # INGREDIENT CONSTRAINT
    # -------------------------

    cold_ingredients = unique_ingredients[
        unique_ingredients["is_cold_storage"] == True  # noqa
    ]["ingredient_id"].tolist()

    # CREATE COLD STORAGE CONSTRAINT
    if cold_ingredients:
        cold_storage_sum = pulp.lpSum(
            [
                ingredient_vars[ing_id]
                for ing_id in cold_ingredients
                if ing_id in ingredient_vars
            ]
        )
        problem += cold_storage_sum <= max_cold_storage

    dry_ingredients = unique_ingredients[
        unique_ingredients["is_cold_storage"] == False  # noqa
    ]["ingredient_id"].tolist()

    # CREATE DRY STORAGE CONSTRAINT
    if dry_ingredients:
        dry_storage_sum = pulp.lpSum(
            [
                ingredient_vars[ing_id]
                for ing_id in dry_ingredients
                if ing_id in ingredient_vars
            ]
        )
        problem += dry_storage_sum <= max_dry_storage

    # -------------------------
    # GLOBAL RECIPE CONSTRAINT
    # -------------------------
    problem += (
        pulp.lpSum(z[rid] for rid in z) == total_required_recipes
    )  # TODO: need a minimum and maximum for this now
    # TODO: minimum = from business input, maximum is from config

    # ---------------------------
    # TAXONOMY MAXIMUM CONSTRAINT
    # ---------------------------
    if company_config.taxonomy_maximum:
        taxonomy_maximum_ids = list(company_config.taxonomy_maximum.keys())

        tax_max = {
            rid: {
                tax: pulp.LpVariable(f"taxmax_{rid}_{tax}", cat="Binary")
                for tax in taxonomy_maximum_ids
                if tax in recipes.recipe_index[rid]["taxonomies"]
            }
            for rid in recipe_ids
        }
        # LINK (STRICTLY) THE MAXIMUM OF A TAXONOMY TO THE GLOBAL RECIPE VARIABLE
        for rid in tax_max:
            for tax in tax_max[rid]:
                problem += tax_max[rid][tax] <= z[rid]
                problem += tax_max[rid][tax] >= z[rid]

        # ADD CONSTRAINT FOR THE TAXONOMY MAXIMUM
        for tax_max_id, maximum in company_config.taxonomy_maximum.items():
            limit_pairs = [
                (rid, tax_max[rid][tax_max_id])
                for rid in tax_max
                if tax_max_id in tax_max[rid]
            ]
            if not limit_pairs:
                error_messages.append(f"No recipes found for taxonomy {tax_max_id}")
                continue
            problem += pulp.lpSum(var for _, var in limit_pairs) <= maximum

    # ------------------------------------------
    # TAXONOMY MINIMUM/BUSINESS RULES CONSTRAINT
    # ------------------------------------------
    for tax_id, rule in taxonomy_constraints.items():
        relevant_pairs = [(rid, x[rid][tax_id]) for rid in x if tax_id in x[rid]]
        if not relevant_pairs:
            error_messages.append(f"No recipes found for taxonomy {tax_id}")
            continue

        problem += pulp.lpSum(var for _, var in relevant_pairs) >= rule.total

        prices = rule.price_category
        for price_id, price_count in prices.items():
            matching_recipes = []
            for rid, var in relevant_pairs:
                recipe_value = recipes.get_recipe(rid).get("price_category_id")
                if recipe_value is not None and recipe_value == price_id:
                    matching_recipes.append(var)

            if len(matching_recipes) == 0:
                error_messages.append(
                    f"No recipes found for price category {price_id} with taxonomy {tax_id}"
                )
                continue
            elif len(matching_recipes) < price_count:
                error_messages.append(
                    f"Found {len(matching_recipes)}/{price_count} recipes for price category {price_id} with taxonomy {tax_id}"
                )

            adjusted_price_count = min(price_count, len(matching_recipes))
            problem += pulp.lpSum(matching_recipes) >= adjusted_price_count

        cooking_times = rule.cooking_time_to
        for cooking_time, cooking_time_count in cooking_times.items():
            matching_recipes = []
            for rid, var in relevant_pairs:
                recipe_value = recipes.get_recipe(rid).get("cooking_time_to")
                if recipe_value is not None and recipe_value <= cooking_time:
                    matching_recipes.append(var)

            if len(matching_recipes) == 0:
                error_messages.append(
                    f"No recipes found for cooking time {cooking_time} with taxonomy {tax_id}"
                )
                continue
            elif len(matching_recipes) < cooking_time_count:
                error_messages.append(
                    f"Found {len(matching_recipes)}/{cooking_time_count} recipes for cooking time {cooking_time} with taxonomy {tax_id}"
                )

            adjusted_cooking_time_count = min(cooking_time_count, len(matching_recipes))
            problem += pulp.lpSum(matching_recipes) >= adjusted_cooking_time_count

        main_ingredients = rule.main_ingredients
        for main_ingredient, main_ingredient_count in main_ingredients.items():
            matching_recipes = []
            for rid, var in relevant_pairs:
                recipe_value = recipes.get_recipe(rid).get("main_ingredient_id")
                if recipe_value is not None and recipe_value == main_ingredient:
                    matching_recipes.append(var)

            if len(matching_recipes) == 0:
                error_messages.append(
                    f"No recipes found for main ingredient {main_ingredient} with taxonomy {tax_id}"
                )

                continue
            elif len(matching_recipes) < main_ingredient_count:
                error_messages.append(
                    f"Found {len(matching_recipes)}/{main_ingredient_count} recipes for main ingredient {main_ingredient} with taxonomy {tax_id}"
                )

            adjusted_main_ing_count = min(main_ingredient_count, len(matching_recipes))
            problem += pulp.lpSum(matching_recipes) >= adjusted_main_ing_count

    # -------------------------
    # LINKED RECIPE EXCLUSION
    # -------------------------
    if linked_recipes:
        for key_recipe, excluded_recipes in linked_recipes.items():
            key_recipe = int(key_recipe)
            if key_recipe in z:
                valid_excluded_recipes = [rid for rid in excluded_recipes if rid in z]
                if valid_excluded_recipes:
                    # if key recipe is selected (z[key_recipe] = 1),
                    # then sum of excluded recipes must be 0
                    # Constraint: z[key_recipe] + sum(excluded) <= 1
                    problem += (
                        z[key_recipe]
                        + pulp.lpSum([z[rid] for rid in valid_excluded_recipes])
                        <= 1
                    )
                    missing_excluded = set(excluded_recipes) - set(
                        valid_excluded_recipes
                    )
                    if missing_excluded:
                        error_messages.append(
                            f"Recipe {key_recipe} excludes non-existent recipes: {list(missing_excluded)}"
                        )

    # -------------------------
    # DIVERSITY VARIABLES
    # -------------------------
    diversity_categories = {category: set() for category in diversity_config}
    recipe_categories = {}

    for rid in recipe_ids:
        recipe_categories[rid] = {}
        recipe = recipes.get_recipe(rid)

        for category in diversity_categories:
            category_value = recipe.get(category)
            if category_value is not None:
                if isinstance(category_value, list):
                    recipe_categories[rid][category] = category_value
                    diversity_categories[category].update(category_value)
                else:
                    recipe_categories[rid][category] = [category_value]
                    diversity_categories[category].add(category_value)
            else:
                recipe_categories[rid][category] = []

    diversity_vars = {}
    diversity_weight = 1.0 / total_required_recipes

    for category in diversity_categories:
        diversity_vars[category] = {}
        for value in diversity_categories[category]:
            if value is None:  # or value == -1 or "unknown"
                continue
            # CREATE DIVERSITY VARIABLES FOR EACH CATEGORY-VALUE PAIR
            diversity_vars[category][value] = pulp.LpVariable(
                f"div_{category}_{value}", cat="Binary"
            )
            # LINK DIVERSITY VARIABLES TO GLOBAL RECIPE VARIABLES
            recipes_with_value = [
                z[rid]
                for rid in recipe_categories
                if rid in z and value in recipe_categories[rid].get(category, [])
            ]

            if recipes_with_value:
                problem += diversity_vars[category][value] <= pulp.lpSum(
                    recipes_with_value
                )

    # -------------------------
    # DIVERSITY OBJECTIVE
    # -------------------------
    objective_terms = []
    for category, category_weight in diversity_config.items():
        if diversity_vars[category]:
            category_diversity = pulp.lpSum(diversity_vars[category].values())
            weighted_diversity = (
                diversity_weight * category_weight
            ) * category_diversity
            objective_terms.append(weighted_diversity)

    if objective_terms:
        problem += pulp.lpSum(objective_terms)

    # -------------------------
    # SOLVER SETUP
    # -------------------------
    solver = pulp.PULP_CBC_CMD(
        msg=CompanyOptimizationConfig.print_msg,
        timeLimit=3,
    )

    problem.solve(solver)

    return problem, ". ".join(error_messages)


def get_selected_recipes(problem: pulp.LpProblem) -> list[int]:
    """
    Extract the recipe IDs that were selected by the solver.

    Args:
        problem (pulp.LpProblem): The solved menu selection problem.

    Returns:
        list[int]: The recipe IDs that were selected.
    """
    recipes = []
    for v in problem.variables():
        if v.name.startswith("z_") and v.value() == 1:
            parts = v.name.split("_")
            recipe_id = int(parts[1])
            recipes.append(recipe_id)

    return recipes


async def generate_menu(
    payload: RequestMenu,
) -> tuple[pulp.LpProblem, pd.DataFrame, str]:
    """
    Create a menu by solving the optimization problem and returning the selected recipes.

    Args:
        payload (RequestMenu): The menu request containing companies and their taxonomy requirements.

    Returns:
        tuple[pulp.LpProblem, pd.DataFrame, str]:
            - The solved PuLP problem.
            - DataFrame of selected recipes (including required and selected recipes).
            - A string of error messages.
    """
    company = payload.companies[0]
    args = Args(company_id=company.company_id, env="prod")
    company_config = CompanyOptimizationConfig.get_company_config(args.company_id)

    df = await create_dataset(args)
    ingredients_df = await get_recipe_ingredients_data()

    available_recipes = list(dict.fromkeys(map(int, company.available_recipes)))
    required_recipes = list(dict.fromkeys(map(int, company.required_recipes)))

    required_recipes_df = pd.DataFrame(df[df["recipe_id"].isin(required_recipes)])
    all_available_recipes_df = pd.DataFrame(df[df["recipe_id"].isin(available_recipes)])
    available_recipes_df = pd.DataFrame(
        all_available_recipes_df[
            ~all_available_recipes_df["recipe_id"].isin(required_recipes)
        ]
    )

    taxonomy_requests, total_req_tax_recipes, taxonomy_error_msg = (
        extract_taxonomy_constraints(payload)
    )

    adjusted_available_recipes, extra_recipes_msg = adjust_available_recipes(
        available_recipe_df=available_recipes_df,
        required_recipe_df=required_recipes_df,
        recipe_universe_df=df,
        taxonomy_constraints=taxonomy_requests,
    )

    problem, error_messages = await solve_menu_and_storage(
        adjusted_available_recipes,  # available_recipes_df
        required_recipes_df,
        ingredients_df,
        taxonomy_requests,
        total_req_tax_recipes,
        company_config,
        linked_recipes=company.linked_recipes,
    )

    selected_recipes = get_selected_recipes(problem)

    final_menu_df = pd.DataFrame(df[df["recipe_id"].isin(selected_recipes)])

    return (
        problem,
        final_menu_df,
        ". ".join(
            filter(None, [extra_recipes_msg + error_messages, taxonomy_error_msg])  #
        ),
    )


def build_response(
    problem: pulp.LpProblem, df: pd.DataFrame, payload: RequestMenu, error_messages: str
) -> ResponseMenu:
    """
    Create a response object summarizing the menu selection results.

    Args:
        problem (pulp.LpProblem): The solved menu selection problem.
        df (pd.DataFrame): DataFrame of selected recipes.
        payload (RequestMenu): The original menu request.
        error_messages (str): A string of error messages.

    Returns:
        ResponseMenu: The response object containing menu details and status.
    """
    companies = []

    for company in payload.companies:
        company_recipes = []

        for _, row in df.iterrows():
            company_recipes.append(
                ReponseRecipe(
                    recipe_id=int(row["recipe_id"]),
                    main_ingredient_id=int(row["main_ingredient_id"]),
                    is_constraint=True,
                )
            )

        taxonomy_results = []
        for taxonomy in company.taxonomies:
            taxonomy_recipes_df = df[
                df["taxonomies"].apply(
                    lambda x: taxonomy.taxonomy_id in x
                    if isinstance(x, list)
                    else False
                )
            ]

            taxonomy_results.append(
                ResponseTaxonomy(
                    taxonomy_id=taxonomy.taxonomy_id,
                    wanted=taxonomy.quantity,
                    actual=len(taxonomy_recipes_df),
                    main_ingredients=[
                        ResponseMainIngredient(
                            main_ingredient_id=ing.main_ingredient_id,
                            wanted=ing.quantity,
                            actual=len(
                                taxonomy_recipes_df[
                                    taxonomy_recipes_df["main_ingredient_id"]
                                    == ing.main_ingredient_id
                                ]
                            ),
                        )
                        for ing in taxonomy.main_ingredients
                    ],
                    price_categories=[
                        ResponsePriceCategory(
                            price_category_id=pc.price_category_id,
                            wanted=pc.quantity,
                            actual=len(
                                taxonomy_recipes_df[
                                    taxonomy_recipes_df["price_category_id"]
                                    == pc.price_category_id
                                ]
                            ),
                        )
                        for pc in taxonomy.price_categories
                    ],
                    cooking_times=[
                        ResponseCookingTime(
                            cooking_time_from=ct.cooking_time_from,  # pyright: ignore
                            cooking_time_to=ct.cooking_time_to,  # pyright: ignore
                            wanted=ct.quantity,
                            actual=len(
                                taxonomy_recipes_df[
                                    taxonomy_recipes_df["cooking_time_to"]
                                    <= ct.cooking_time_to
                                ]
                            ),
                        )
                        for ct in taxonomy.cooking_times
                    ],
                    average_rating=ResponseAverageRating(
                        average_rating=taxonomy.min_average_rating,
                        wanted=taxonomy.quantity,
                        actual=len(
                            taxonomy_recipes_df[
                                taxonomy_recipes_df["average_rating"]
                                >= taxonomy.min_average_rating
                            ]
                        ),
                    ),
                )
            )

        companies.append(
            ResponseCompany(
                company_id=company.company_id,
                recipes=company_recipes,
                taxonomies=taxonomy_results,
            )
        )

    return ResponseMenu(
        week=payload.week,
        year=payload.year,
        companies=companies,
        status=problem.status,
        status_msg=pulp.LpStatus[problem.status] + ". " + error_messages,
    )


if __name__ == "__main__":
    import asyncio
    import json
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv())

    test_file_path = "../menu_optimiser/test_files/linked/rt_45_link.json"

    with open(test_file_path, "r") as f:
        raw_data = json.load(f)
    payload = RequestMenu.model_validate(raw_data)

    problem, final_menu_df, error_messages = asyncio.run(generate_menu(payload))
