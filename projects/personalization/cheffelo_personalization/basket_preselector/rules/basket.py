"""
Contains the hard rules to choose the preselected dishes, with rules being hard rules.

Types of hard rules:
- Chef defining rules, contains the rules defined by chefs, for example minimum one dish of potatoes and maximum 3
- Quarantine rules, dishes that are already selected in earlier weeks for this customer should have lower priority
"""
import logging
from collections import Counter
from itertools import chain, combinations, islice
from typing import Dict, Tuple

import pandas as pd

from cheffelo_personalization.basket_preselector.utils.constants import (
    PROTEINS_AVAILABLE,
)

logger = logging.getLogger(__name__)

cost_categories = [
    {"min": 0, "max": 99, "pct": 0.2},
    {"min": 100, "max": 109, "pct": 0.4},
    {"min": 110, "max": 120, "pct": 0.2},
    {"min": 120, "max": 200, "pct": 0.2},
]


def check_dish_combination(
    possible_basket: pd.DataFrame,
    customer: pd.Series,
    df_preference_rules: pd.DataFrame,
    run_config: Dict,
    combo_debug_summary: Dict,
) -> Tuple[int, Dict]:
    """Check the dish combination (possible basket) against different rules to see if it breaks them

    Args:
        possible_basket (pd.DataFrame): The dish combination that makes the possible basket
        customer (Customer): Customer information
        df_preference_rules (pd.DataFrame): dataframe of preference rules
        combo_debug_summary (Dict): dictionary of log

    Returns:
        Tuple: number of rules broken with the dictionary of logs
    """
    total_rules_broken = 0

    # Count the variety of preferences
    preference_variety_count = count_preference_variety(possible_basket)

    combo_debug_summary["basket_preference_rules"] = str(
        run_config["rules"]["preference_rules"]
    )
    combo_debug_summary["basket_protein_variety"] = str(
        run_config["rules"]["protein_variety"]
    )

    # Check preference rules
    if run_config["rules"]["preference_rules"]:
        preference_rules_broken, combo_debug_summary = check_preference_rules(
            preference_variety_count, df_preference_rules, combo_debug_summary
        )
        combo_debug_summary["basket_preference_rules_broken"] = preference_rules_broken
        total_rules_broken += preference_rules_broken

    # Check repeated proteins
    if run_config["rules"]["protein_variety"]:
        proteins_repeated, combo_debug_summary = check_protein_variety(
            preference_variety_count, combo_debug_summary
        )
        total_rules_broken += proteins_repeated

    # Check cost of food
    if run_config["rules"]["cost_of_food"]:
        proteins_repeated, combo_debug_summary = check_cost_of_food(
            preference_variety_count, combo_debug_summary
        )
        total_rules_broken += proteins_repeated

    return total_rules_broken, combo_debug_summary


def count_preference_variety(dish_combination: pd.DataFrame) -> Counter:
    """
    Counts the variety of preferences in a dish combination.

    Parameters:
    - dish_combination (pd.DataFrame): A DataFrame containing information about dish combinations.

    Returns:
    - Counter: A Counter object that counts the occurrences of each preference.
    """
    # Flatten 2d list of preferences to 1d
    preferences = list(chain.from_iterable(dish_combination["preference_ids"].tolist()))

    # Count occurrences of each preference
    return Counter(preferences)


def check_preference_rules(
    preference_variety_count: Counter,
    preference_rules: pd.DataFrame,
    combo_debug_summary: Dict,
) -> Tuple[int, Dict]:
    """
    Check if preference rules are broken for each preference in the given dataset.

    Args:
        preference_variety_count (Counter): A Counter object containing the count of each preference variety.
        preference_rules (pd.DataFrame): A DataFrame containing the preference rules.
        combo_debug_summary (Dict): A dictionary containing the debug summary.

    Returns:
        Tuple[int, Dict]: A tuple containing the number of broken rules and the updated debug summary.
    """
    rules_broken = 0

    # Go through each rule and check if a rule is broken
    for _, rule in preference_rules.iterrows():
        preference = rule["preference_id"]
        lower_value = rule["lower_rule_value"]
        upper_value = rule["upper_rule_value"]
        if preference in preference_variety_count:
            preference_count = preference_variety_count[preference]
            if (preference_count < lower_value) or (preference_count > upper_value):
                rules_broken += 1

    return rules_broken, combo_debug_summary


def check_protein_variety(
    preference_variety_count: Counter, combo_debug_summary: Dict
) -> Tuple[int, Dict]:
    """Checks the protein variety in the combination passed.
    The idea is to have a balance of different proteins and not repeat the same protein twice.


    Args:
        possible_dishes (pd.DataFrame): _description_

    Returns:
        int: _description_
    """
    proteins_existing = {
        c: preference_variety_count[c]
        for c in preference_variety_count.keys()
        if c in PROTEINS_AVAILABLE
    }

    # Idea is to not have repeated proteins, one rule broken for each repeated protein
    num_unique_proteins = len(proteins_existing)
    num_repeated_proteins = len(
        [
            preference
            for preference, occurrences in proteins_existing.items()
            if occurrences > 1
        ]
    )
    combo_debug_summary["basket_proteins_unique"] = num_unique_proteins
    combo_debug_summary["basket_proteins_repeated"] = num_repeated_proteins
    return num_repeated_proteins, combo_debug_summary


def check_cost_of_food(
    dish_combination: pd.DataFrame, combo_debug_summary: Dict
) -> Tuple[int, Dict]:
    """
    Compute the cost of food for a given dish combination.

    Args:
        dish_combination (pd.DataFrame): A DataFrame containing information about the dish combination.
        combo_debug_summary (Dict): A dictionary containing debug information about the combination.

    Returns:
        Tuple[int, Dict]: A tuple containing the number of rules broken and the updated combo debug summary.
    """
    num_rules_broken = 0
    cost_of_food = dish_combination["variation_price"].sum()
    combo_debug_summary["basket_cost_of_food"] = cost_of_food
    return num_rules_broken, combo_debug_summary


def filter_combinations_based_on_margin_targets(
    possible_dishes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Select possible combination of food based on the cost categories.

    Args:
        possible_dishes (_type_): _description_

    Returns:
        list: _description_
    """
    return possible_dishes


def generate_basket_combinations(
    possible_dishes: pd.DataFrame,
    number_of_dishes: int,
    run_config: Dict,
    max_combinations: int = 100,
) -> pd.DataFrame:
    """
    Generate combinations of possible dishes.

    Args:
        possible_dishes (pd.DataFrame): A DataFrame containing the possible dishes.
        number_of_dishes (int): The number of dishes to include in each combination.
        max_combinations (int, optional): The maximum number of combinations to generate. Defaults to 100.

    Returns:
        pd.DataFrame: A DataFrame containing the generated combinations.
    """
    possible_dishes_index = possible_dishes.reset_index().index.tolist()
    combos = list(
        islice(combinations(possible_dishes_index, number_of_dishes), max_combinations)
    )
    return combos


def score_combination() -> pd.DataFrame:
    return pd.DataFrame([])
