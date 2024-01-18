"""
The rule engine class and helpers
"""

import os
import time
from collections import Counter
from itertools import chain, combinations

import pandas as pd

import cheffelo_personalization.utils.constants as constants
from cheffelo_personalization.utils.log import log_handler

logger, _ = log_handler.initialize_logging(
    os.environ.get("LOG_LEVEL", "DEBUG"), __name__
)


class RulesEngine:
    """
    The main objective of the rule engine is to
    - Maximize the number of rules fulfilled
    - Create more diversity in the dishes being recommended (protein variety)
    - Lower the possibility of recommending same dish multiple weeks in a row (quarantine)

    The rule engine do not remove dishes as possible recommendations,
    it only reorders the recommended order as a way to get best possible combo to the customer
    """

    def __init__(self, filtered, basket_product_id, rules):
        """
        Takes in a Filter object for a agreement_id, the possible rules and basket_product_id
        """
        self.filtered = filtered
        self.basket_product_id = basket_product_id
        self.rules = rules

    def process_rules(self):
        """Loops over the weeks to run the rule engine and adds the best combination as a deviation"""
        logger.debug("Running rules engine...")
        for i in self.filtered.accepted_deviations.index:
            # Fetch the year, week and basket size for the deviation
            (
                current_yearweek,
                basket_size,
            ) = self.filtered.selector_deviations.loc[
                i, ["yearweek", "expected_dishes"]
            ]

            # Filter the selector pool on yearweek
            selector_pool_yearweek = self.filtered.selector_pool.loc[
                (self.filtered.selector_pool["yearweek"] == current_yearweek)
            ]

            # Get the deviation product preference for given year week and that is not removed by selector
            deviation_product_pref_yearweek = self.filtered.deviation_products.loc[
                (self.filtered.deviation_products["selector_removed"] == 0)
                & (self.filtered.deviation_products["yearweek"] == current_yearweek),
                "preferences",
            ]
            deviation_product_pref_list = list(
                chain.from_iterable(deviation_product_pref_yearweek)
            )

            # If we have a combination size larger than 0
            comb_size = self.filtered.selector_deviations.loc[i, "max_comb_size"]
            if comb_size < 1:
                continue

            combos = self.create_dish_combinations(current_yearweek, comb_size)
            possible_rules = self.get_possible_rules(
                deviation_product_pref_yearweek, current_yearweek
            )
            i = 0
            if len(possible_rules) == 0:
                # Just return the first combination
                combo_score = [(combos[0], 0, (0, True), (0, True))]
            else:
                # If there are possible rules, loop over the alternative combos to find best combo
                combo_score = self.calculate_combo_score(
                    combos,
                    selector_pool_yearweek,
                    deviation_product_pref_list,
                    possible_rules,
                    basket_size,
                    current_yearweek,
                )

            combo_score = sort_combo_score(combo_score)

            self.add_combination_to_selector_deviation_products(
                current_yearweek, combo_score[0][0]
            )

    def calculate_combo_score(
        self,
        combos,
        selector_pool_yearweek,
        deviation_product_pref_list,
        possible_rules,
        basket_size,
        current_yearweek,
    ):
        """Calculates the combo score for each combo or until all rules fulfilled"""
        # Start timer to avoid being stuck in a forever loop
        timer = time.time()
        combo_score = []
        for combo in combos:
            # Fetch the preference and
            pool_combo = selector_pool_yearweek.loc[
                selector_pool_yearweek["order_of_relevance"].isin(combo),
                ["preferences", "main_recipe_ids"],
            ]

            # Count the combination of preferences
            pref_combo_count = count_preference_combo(
                pool_combo["preferences"],
                deviation_product_pref_list,
            )

            # Check number of rules fulfilled
            (
                rules_fulfilled,
                total_number_of_rules,
            ) = check_rules_fulfillment(pref_combo_count, possible_rules)

            # Count the variety of proteins
            count_variety = count_variety_combo(pref_combo_count, basket_size)

            repeated_dishes = check_quarantine_dishes(
                pool_combo["main_recipe_ids"],
                current_yearweek,
                self.filtered.selector_deviation_products[
                    ["yearweek", "main_recipe_ids"]
                ],
            )

            # Add results to list of scores
            combo_score.append(
                (
                    combo,
                    rules_fulfilled,
                    count_variety,
                    repeated_dishes,
                )
            )

            # If all rules fulfilled, there are no better solution, thus break the for loop
            if rules_fulfilled / total_number_of_rules == 1:
                logger.debug("All rules fulfilled. Continuing...")
                break

            # If maximum time exceeded, break for loop. Is ment to avoid looping forever.
            if time.time() - timer > 1.5:
                logger.warning("Maximum time per deviation reached. Continuing...")
                break

        return combo_score

    def add_combination_to_selector_deviation_products(
        self, current_yearweek, combination
    ):
        """returns a dataframe with variations from a list of combinations"""
        variations = self.filtered.selector_pool.loc[
            (self.filtered.selector_pool["yearweek"] == current_yearweek)
            & (self.filtered.selector_pool["order_of_relevance"].isin(combination)),
            ["yearweek", "variation_id", "main_recipe_ids"],  # "year", "week",
        ]
        variations["quantity"] = 1

        self.filtered.selector_deviation_products = pd.concat(
            [self.filtered.selector_deviation_products, variations]
        )

    def get_possible_rules(self, deviation_product_pref_yearweek, current_yearweek):
        """
        Takes in all rules and filters them based on
        what is possible (checking selector_pool and selector_deviation_dishes)
        """

        pool_preferences = self.filtered.pool.loc[
            (self.filtered.pool["selector_removed"] == 0)
            & (self.filtered.pool["yearweek"] == current_yearweek),
            "preferences",
        ]
        unique_preferences = set(
            chain.from_iterable(deviation_product_pref_yearweek)
        ).union(set(chain.from_iterable(pool_preferences)))
        try:
            possible_rules = [
                rule
                for rule in self.rules
                if rule["preference_id"] in unique_preferences
            ]
        except Exception as err:
            logger.exception(err)
            logger.error("Failed to fetch possible rules")
            raise err

        return possible_rules

    def create_dish_combinations(self, current_yearweek, number_of_dishes):
        """
        Create a new dish combination from the selected pool and for a given week
        """
        combos = list(
            combinations(
                self.filtered.selector_pool.loc[
                    (self.filtered.selector_pool["yearweek"] == current_yearweek),
                    "order_of_relevance",
                ],
                number_of_dishes,
            )
        )
        # Order combinations based on total scoring
        combos_df = pd.DataFrame(combos)
        combos_df["sum"] = combos_df[list(combos_df.columns)].sum(axis=1)
        combos = (
            combos_df.sort_values(by=["sum"], ascending=True)
            .drop(columns=["sum"])
            .values.tolist()
        )

        return combos


def count_preference_combo(pool_preference_combo, deviation_product_pref_list):
    """
    Count the number of preference combination
    """
    pool_pref_combo_list = list(chain.from_iterable(pool_preference_combo))
    return Counter(deviation_product_pref_list + pool_pref_combo_list)


def check_rules_fulfillment(pref_comb, rules):
    """
    Check how many rules are fulfilled
    """
    rules_fulfilled = 0
    total_number_of_rules = len(rules)

    for rule in rules:
        if rule["type"] != "count":
            continue

        rule_value = int(rule["rule_value"])
        rule_type = rule["rule_type"]
        preference = rule["preference_id"]
        if (
            (rule_type == "lower_bound")
            and (rule_value <= pref_comb[preference])
            and (preference in pref_comb)
        ):
            rules_fulfilled += 1
        if (
            (rule_type == "upper_bound")
            and (rule_value >= pref_comb[preference])
            and (preference in pref_comb)
        ):
            rules_fulfilled += 1

    return rules_fulfilled, total_number_of_rules


def sort_combo_score(combo_score):
    """Sort the combo score by number of rules, variety of protein and quarantine"""
    # Sort combos by rule score
    combo_score.sort(reverse=True, key=lambda c: c[1])

    # Try and extra sort combos based on variety of protein
    combo_score.sort(reverse=True, key=lambda c: c[2][1])

    # Extra sort combos based if the dishes are repeated
    combo_score.sort(reverse=False, key=lambda c: c[3][0])

    return combo_score


def count_variety_combo(pref_comb: Counter, basket_size):
    """
    Count protein variety
    """
    proteins_existing = {
        c: pref_comb[c] for c in pref_comb.keys() if c in constants.PROTEINS_AVAILABLE
    }

    # Bad combo -> 1 protein (sum of dishes proteins == basket size)
    if len(proteins_existing) == 1 and sum(proteins_existing.values()) == basket_size:
        flag = False
    else:
        flag = True

    return len(proteins_existing), flag


def check_quarantine_dishes(
    recipe_pool_combo,
    current_yearweek,
    selector_deviation_products_recipe,
    quarantine_length=4,
):
    """
    Check the combo and compare the recommendations for previous weeks,
    if same main_recipe_ids has been recommended move the dish until the end.
    Only check for recipes bought less than quarantine length.

    - quarantine_length is the length of the quarantine, meaning that any
    dishes that has been recommended the last n weeks should not be recommended again.
    """
    num_repeated_dishes = 0
    flag = True
    try:
        # Filter out recipes ordered longer than the quarantine length
        deviation_main_recipe_ids = selector_deviation_products_recipe[
            selector_deviation_products_recipe["yearweek"]
            >= current_yearweek - quarantine_length
        ]["main_recipe_ids"].dropna()

        deviation_main_recipe_ids = list(chain.from_iterable(deviation_main_recipe_ids))
        recipe_pool_combo = list(chain.from_iterable(recipe_pool_combo))

        num_repeated_dishes = len(
            set(recipe_pool_combo) & set(deviation_main_recipe_ids)
        )

        if num_repeated_dishes > 0:
            flag = False
    except Exception as err:
        logger.warning("Failed to check quarantine dishes with error %s", err)

    return num_repeated_dishes, flag
