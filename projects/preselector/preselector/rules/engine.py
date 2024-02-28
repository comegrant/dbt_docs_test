"""
Contains the functions to filter and score the dishes and find the best dish combination to generate a basket. Can
"""
import logging
import time
from typing import Any

import pandas as pd

from preselector.data.models.customer import PreselectorCustomer, RunConfig

from .basket import check_dish_combination, generate_basket_combinations
from .filter import filter_portion_size, filter_taste_restrictions
from .ranking import rank_dishes_based_on_quarantine, rank_dishes_basked_on_rec_engine

logger = logging.getLogger()


def run_preselector_engine(
    customer: PreselectorCustomer,
    df_flex_products: pd.DataFrame,
    df_rec: pd.DataFrame,
    df_preference_rules: pd.DataFrame,
    df_quarantined_dishes_for_customer: pd.DataFrame,
    run_config: RunConfig,
) -> tuple[pd.DataFrame, dict] | Exception:
    """
        Run the preselector engine

    Args:
        customer (pd.Series): Series of customer information
        df_flex_products (pd.DataFrame): All possible dishes for the week

    Returns:
        _type_: _description_
    """
    debug_summary: dict[str, int] = {}
    result = run_filter_rules(
        possible_dishes=df_flex_products,
        customer=customer,
        run_config=run_config,
        debug_summary=debug_summary,
    )

    if isinstance(result, Exception):
        return result
    possible_dishes, debug_summary = result

    num_dishes = customer.number_of_recipes
    if len(possible_dishes) < num_dishes:
        logger.warning(
            "Not enough possible dishes for number of meals for customer %s",
            customer.agreement_id,
        )
        num_dishes = len(possible_dishes)

        # If number of possible dishes is less than number of dishes asked by customer, return the possible dishes
        return possible_dishes.sample(n=num_dishes)["main_recipe_id"]

    # Run ranking rules to rank the dishes in order of relevance
    possible_dishes, debug_summary = run_ranking_rules(
        possible_dishes=possible_dishes,
        customer=customer,
        df_rec=df_rec,
        df_quarantined_dishes_for_customer=df_quarantined_dishes_for_customer,
        run_config=run_config,
        debug_summary=debug_summary,
    )

    # Create combination of dishes and check if their combination attributes comply with the basket rules
    basket_combination, debug_summary = run_basket_rules(
        possible_dishes=possible_dishes,
        customer=customer,
        df_preference_rules=df_preference_rules,
        run_config=run_config,
        debug_summary=debug_summary,
    )

    return basket_combination, debug_summary


def run_filter_rules(
    possible_dishes: pd.DataFrame,
    customer: PreselectorCustomer,
    run_config: RunConfig,
    debug_summary: dict,
) -> tuple[pd.DataFrame, dict] | Exception:
    """
        Filters out dishes that does not comply with the hard rules and returns a filtered list

    Args:
        dishes (list): a list of dictionary with the dish information
        customer_information (dict): a list of dictionary with the dish information
        menu (list): a list of all dishes in the current menu week

    Returns:
        pd.DataFrame: dataframe of possible dishes
    """
    if run_config.should_filter_portion_size:
        result = filter_portion_size(
            possible_dishes, customer.portion_size, debug_summary,
        )

        if isinstance(result, Exception):
            return result

        possible_dishes, debug_summary = result

    if run_config.should_filter_taste_restrictions:
        result = filter_taste_restrictions(
            possible_dishes, customer.taste_preference_ids, debug_summary,
        )

        if isinstance(result, Exception):
            return result

        possible_dishes, debug_summary = result

    return possible_dishes, debug_summary


def run_ranking_rules(
    possible_dishes: pd.DataFrame,
    customer: PreselectorCustomer,
    df_rec: pd.DataFrame,
    df_quarantined_dishes_for_customer: pd.DataFrame,
    run_config: RunConfig,
    debug_summary: dict,
) -> tuple[pd.DataFrame, dict]:
    """
    Scores all of the possible dishes based on a set of soft rules, for example quarantine where we check if a dish has been chosen previously.

    Args:
        customer_information (dict): _description_
        possible_dishes (list): _description_

    Returns:
        list: _description_
    """
    possible_dishes = possible_dishes.assign(score=0)

    if run_config.should_rank_using_rec_engine:
        possible_dishes, debug_summary = rank_dishes_basked_on_rec_engine(
            possible_dishes, df_rec, debug_summary,
        )

    if run_config.should_rank_with_quarantine:
        possible_dishes, debug_summary = rank_dishes_based_on_quarantine(
            possible_dishes, df_quarantined_dishes_for_customer, debug_summary,
        )

    return possible_dishes, debug_summary


def run_basket_rules(
    possible_dishes: pd.DataFrame,
    customer: PreselectorCustomer,
    df_preference_rules: pd.DataFrame,
    run_config: RunConfig,
    debug_summary: dict,
) -> tuple[pd.DataFrame, dict]:
    """

    Args:
        possible_dishes (list): All possible dishes that can be combined to generate a basket
        customer (dict): Information on the customer, such as maximum number of dishes in basket

    Returns:
        list: The dish combination that together makes up the basket
    """

    # Should loop over and try different combination of dishes until we found a combination that can't be improved or we timeout
    best_scored_combination = 9999999
    timelimit_reached = 0
    all_rules_fulfilled = False
    max_time = 0.5  # in seconds
    best_basket_combination = pd.DataFrame()

    # Filter out only relevant preference rules
    if df_preference_rules.empty:
        df_preference_rules_for_product = df_preference_rules
    else:
        df_preference_rules_for_product = df_preference_rules[
            df_preference_rules["product_id"]
            == customer.subscribed_product_variation_id
        ]

    # Get the first combination
    start_time = time.time()
    basket_combinations = generate_basket_combinations(
        possible_dishes, customer.number_of_recipes,
    )
    debug_summary["basket_combinations_time"] = time.time() - start_time
    debug_summary["basket_combinations_length"] = len(basket_combinations)

    combination_position = list(basket_combinations.pop())
    possible_basket = possible_dishes.iloc[combination_position]
    rounds = 0
    start_time = time.time()
    while not (all_rules_fulfilled or timelimit_reached):
        # For each of the different combination
        round_time = time.time()
        combo_debug_summary: dict[str, Any] = {}

        # Go through the rules and count the number of rules that are broken
        rounds = rounds + 1
        total_rules_broken, combo_debug_summary = check_dish_combination(
            possible_basket,
            df_preference_rules_for_product,
            run_config,
            combo_debug_summary,
        )

        # If no rules are broken, we return the current basket combination
        all_rules_fulfilled = total_rules_broken == 0

        # If timelimit reached, we return with the best basket combination
        time_spent = time.time() - start_time
        combo_debug_summary["basket_round_time_spend"] = time.time() - round_time
        if time_spent > max_time:
            timelimit_reached = 1
            if best_basket_combination.empty:
                best_basket_combination = possible_basket
                best_combination_log = combo_debug_summary
            break

        # If the current combination is the best scored yet, set as the best combination
        if total_rules_broken < best_scored_combination:
            best_basket_combination = possible_basket
            best_scored_combination = total_rules_broken
            best_combination_log = combo_debug_summary
            best_combination_log["basket_best_combo_rounds"] = rounds
            best_combination_log["basket_best_combo_position"] = combination_position

        # If there is no more basket combinations to go through, break the loop
        if len(basket_combinations) == 0:
            break

        # Pop the next top scored combination from a list
        combination_position = list(basket_combinations.pop())
        possible_basket = possible_dishes.iloc[combination_position]

    best_combination_log["basket_timelimit_reached"] = timelimit_reached
    best_combination_log["basket_time_spend"] = time_spent
    best_combination_log["basket_rounds"] = rounds
    debug_summary.update(best_combination_log)
    return best_basket_combination, debug_summary
