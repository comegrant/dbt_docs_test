import os
from typing import List

import pandas as pd

import cheffelo_personalization.meal_selector.engine.parser_module as pm
import cheffelo_personalization.meal_selector.utils.constants as constants
from cheffelo_personalization.utils.log import log_handler

logger, _ = log_handler.initialize_logging(
    os.environ.get("LOG_LEVEL", "DEBUG"), __name__
)


class Filter:
    def __init__(
        self,
        cms: pd.DataFrame,
        psl: pd.DataFrame,
        pim: pd.DataFrame,
        preselected: pd.DataFrame,
        weeks: List,
    ):
        logger.debug("Creating Filter object")
        self.cms_parsed = cms
        self.weeks = weeks
        self.psl_parsed = psl
        self.pim_parsed = pim
        self.preselected_parsed = preselected

        # DataFrames to be declared
        self.basket = None
        self.basket_product = None
        self.basket_other_products = None
        self.deviation_products = pd.DataFrame(
            columns=[
                # "year",
                # "week",
                "product_id",
                "variation_id",
                "name",
                "portions",
                "meals",
                "price",
                "preferences",
                "preselected_product_id",
                "recipe_order",
            ]
        )
        self.pool = pd.DataFrame(
            columns=[
                # "year",
                # "week",
                "product_id",
                "variation_id",
                "name",
                "portions",
                "meals",
                "price",
                "preferences",
                "preselected_product_id",
                "recipe_order",
            ]
        )

        # Generated from this module
        self.selector_pool = None
        self.selector_deviations = None
        self.selector_deviation_products = None
        self.accepted_deviations = None

    def run_filtering_process(self, batch="selector"):
        """
        Main function. Executing the other functions
        """
        # Initialization
        logger.debug("Running filtering process...")
        self.get_basket()
        self.get_basket_products()
        self.get_pool_and_default_dishes()

        # Execution
        self.create_selector_deviations()
        self.filter_dishes_by_basket_preference()
        self.add_score_to_selector_pool(batch)

        self.add_basket_other_products_to_selector_deviation()
        self.filter_selector_deviation()

    def get_psl_data_by_product_type(self, product_type_id):
        """Filters the psl data by product_type_id"""
        base_fields = [
            # "year",
            # "week",
            "yearweek",
            "product_id",
            "variation_id",
            "name",
            "portions",
            "meals",
        ]

        if product_type_id == constants.MEALBOX_ID:
            logger.debug("Product type is Mealbox")
            spec_fields = ["flex_financial_variation_id"]
            # Avoid using mealboxes that dont have flex_financial_variation_id
            data = self.psl_parsed.loc[
                (self.psl_parsed["product_type_id"] == product_type_id)
                & (
                    self.psl_parsed["flex_financial_variation_id"]
                    != constants.FLEX_FINANCIAL_VARIATION_ID_DEFAULT
                ),
                base_fields + spec_fields,
            ]

        elif product_type_id == constants.VELG_VRAK_ID:
            logger.debug("Product type is VelgVrak")
            spec_fields = ["price"]
            data = self.psl_parsed.loc[
                self.psl_parsed["product_type_id"] == product_type_id,
                base_fields + spec_fields,
            ]
        else:
            logger.debug("Product type if neither Mealbox nor Velg Vrak")
            data = pd.DataFrame(columns=base_fields)

        # data["yearweek"] = data["year"] * 100 + data["week"]

        return data

    def get_pim_data_for_basket_portion(self):
        """Returns pim data for the portion size of the basket"""
        return self.pim_parsed.loc[
            self.pim_parsed["portions"] == str(self.basket_product["portions"])
        ]

    def get_basket(self):
        """Creates a dict with basket information"""
        self.basket = {
            "agreement_id": self.cms_parsed["agreement_id"],
            "company_id": self.cms_parsed["company_id"],
            "preferences": self.cms_parsed["preferences"],
        }

    def get_basket_products(self):
        """
        Gets basket product (in future: concept) to run Filtering for.
        Also creates basket_other_products (leftovers in basket to add to selector deviations)
        Will be per week (because we're using the PSL data)
        """
        logger.debug("Getting basked products")
        products = self.cms_parsed["products"].copy()
        unique_product_variation_ids = set(
            product["variation_id"] for product in products if product["quantity"] > 0
        )
        all_mealboxes = self.get_psl_data_by_product_type(constants.MEALBOX_ID)

        # If more than 1 mealbox, just pick the first one on the list
        for variation in unique_product_variation_ids:
            if variation in all_mealboxes["variation_id"].values:
                logger.debug(
                    "Picking the first mealbox variation for variation %s", variation
                )

                mealbox_variation = variation
                break
        else:
            logger.error("No variation found in mealbox found.")
            mealbox_variation = None

        if mealbox_variation:
            logger.debug("Variation is a mealbox variation")
            mealbox_psl = all_mealboxes.loc[
                all_mealboxes["variation_id"] == mealbox_variation
            ]

            self.basket_product = {
                "product_id": mealbox_psl["product_id"].values[0],
                "variation_id": mealbox_psl["variation_id"].values[0],
                "financial_variation_id": mealbox_psl[
                    "flex_financial_variation_id"
                ].values[0],
                "meals": int(mealbox_psl["meals"].values[0]),
                "portions": mealbox_psl["portions"].values[0],
                "quantity": 1,
            }

            self.basket_other_products = pd.DataFrame(
                products, columns=["variation_id", "quantity"]
            )

            # Find mealbox and subtract 1 in quantity
            mealbox_index = (
                (self.basket_other_products["variation_id"] == mealbox_variation)
                & (self.basket_other_products["quantity"] > 0)
            ).idxmax()
            self.basket_other_products.loc[mealbox_index, "quantity"] = (
                self.basket_other_products.loc[mealbox_index, "quantity"] - 1
            )

            # Remove all products with quantity = 0
            self.basket_other_products = self.basket_other_products.loc[
                self.basket_other_products["quantity"] > 0, :
            ].reset_index(drop=True)

    def get_preselected_products_for_basket_product(self):
        """Subsets the preselected data to only have the data for the mealbox we're modeling for"""
        return self.preselected_parsed.loc[
            self.preselected_parsed["product_id"] == self.basket_product["product_id"],
            ["yearweek", "preselected_product_id", "recipe_order"],  # "year", "week",
        ]

    def get_pool_and_default_dishes(self):
        """Gets pool of dishes from PSL and combines with PIM data"""
        if self.basket_product:
            logger.debug("Has basket product, inserting pool and default dishes")
            all_products_psl = self.get_psl_data_by_product_type(constants.VELG_VRAK_ID)
            all_products_psl_portion = all_products_psl.loc[
                all_products_psl["portions"] == self.basket_product["portions"]
            ]

            all_recipes_pim_portion = self.get_pim_data_for_basket_portion()

            dishes = all_products_psl_portion.merge(
                all_recipes_pim_portion,
                how="inner",
                on=[
                    # "year",
                    # "week",
                    "yearweek",
                    "product_id",
                    "variation_id",
                    "portions",
                ],
            )

            # New logic to create the pool deviation products
            preselected_products = self.get_preselected_products_for_basket_product()
            dishes = pd.merge(
                dishes.loc[
                    :,
                    [
                        # "year",
                        # "week",
                        "yearweek",
                        "product_id",
                        "variation_id",
                        "name",
                        "portions",
                        "meals",
                        "price",
                        "preferences",
                        "main_recipe_ids",
                    ],
                ],
                preselected_products,
                how="left",
                left_on=["yearweek", "product_id"],
                right_on=["yearweek", "preselected_product_id"],
            )
            dishes.loc[dishes["recipe_order"].isna(), "recipe_order"] = 0
            self.deviation_products = dishes.loc[
                (dishes["recipe_order"] > 0)
                & (dishes["recipe_order"] <= self.basket_product["meals"])
            ]

            self.pool = dishes.loc[
                (dishes["recipe_order"] > self.basket_product["meals"])
                | ((dishes["recipe_order"] == 0) & (dishes["price"] == 0))
            ]
        try:
            assert not self.deviation_products.empty
            assert not self.pool.empty
        except AssertionError as err:
            logger.exception(err)
            logger.error("Deviation products or pool are empty. Continuing...")

    def create_selector_deviations(self):
        """Creates deviations for each week requested. Requires a basket product"""
        self.selector_deviations = pd.DataFrame(self.weeks)
        self.selector_deviations["yearweek"] = (
            self.selector_deviations["year"] * 100 + self.selector_deviations["week"]
        )
        if self.basket_product:
            logger.debug("Setting expected dishes to basket product meals")
            self.selector_deviations["expected_dishes"] = self.basket_product["meals"]
        else:
            logger.debug("No basket product; setting expected dishes to 0")
            self.selector_deviations["expected_dishes"] = 0
            logger.error(
                "Variation_id not found in basket for agreement_id %s",
                str(self.basket["agreement_id"]),
            )

        self.selector_deviations["result"] = None

    def get_alternative_financial(self, meals):
        """
        Returns a financial variation_id with same product_id and portion_size,
        but with different number of meals (if exists).
        """
        # THIS IS HOW IT IS TEMPORARY (TO GET IT TO WORK WITHOUT THE BELOW COMMENTS)
        portions = self.basket_product["portions"]
        product_id = self.basket_product["product_id"]

        psl_mealboxes = self.get_psl_data_by_product_type(constants.MEALBOX_ID)

        alt_financial = psl_mealboxes.loc[
            (psl_mealboxes["product_id"] == product_id)
            & (psl_mealboxes["portions"] == portions)
            & (psl_mealboxes["meals"] == str(meals)),
            "flex_financial_variation_id",
        ].values

        if alt_financial.size == 0:
            logger.warning(
                "Financial alternative is empty for agreement_id %s",
                str(self.basket["agreement_id"]),
            )
            return None
        else:
            return alt_financial[0]

    def filter_dishes_by_basket_preference(self):
        """
        Filters deviation_products and pool by preference
        Inserts dishes into selector_deviation_products
        """
        logger.debug("Filtering deviation and pool products by preference")
        # Initially everything is cool
        start_deviation_products = self.deviation_products.copy()
        start_pool = self.pool.copy()

        self.deviation_products["selector_removed"] = 0
        self.pool["selector_removed"] = 0

        # Remove dishes with negative taste preferences from deviation products
        start_deviation_products = self.remove_negative_dish_preferences_basket(
            start_deviation_products
        )

        # Remove dishes with negative taste preferences from the selector pool
        start_pool = self.remove_negative_dish_preferences_selector(start_pool)

        # Update selector pool and deviation products after removing dishes with negative taste preferences
        self.update_selector_pool_after_removing_dishes(
            start_deviation_products, start_pool
        )

        #
        self.set_selector_result()

    def remove_negative_dish_preferences_basket(self, start_deviation_products):
        """Remove the dishes with negatative taste preferences"""
        for ind in self.deviation_products.index:
            for pref in self.deviation_products["preferences"][ind]:
                if pref in self.basket["preferences"][constants.TASTE_PREFERENCE]:
                    start_deviation_products.drop(ind, inplace=True)
                    self.deviation_products.loc[ind, "selector_removed"] = 1
                    break

        if self.deviation_products["selector_removed"].sum() == 0:
            logger.debug(
                "No negative taste preferences detected; no dishes removed from deviation products"
            )
        else:
            logger.debug(
                "In total %s dishes removed from deviation products due to negative taste preference",
                self.deviation_products["selector_removed"].sum(),
            )

        return start_deviation_products

    def remove_negative_dish_preferences_selector(self, start_pool):
        """Removing dishes from selector pool with negative taste preferences"""
        for ind in self.pool.index:
            for pref in self.pool["preferences"][ind]:
                if pref in self.basket["preferences"][constants.TASTE_PREFERENCE]:
                    start_pool.drop(ind, inplace=True)
                    self.pool.loc[ind, "selector_removed"] = 1
                    break

        if self.pool["selector_removed"].sum() == 0:
            logger.debug("No dishes removed from selector pool")

        return start_pool

    def update_selector_pool_after_removing_dishes(
        self, start_deviation_products, start_pool
    ):
        """Update the selector deviation with the new info"""
        # Count default dishes left
        selector_deviation_status = (
            start_deviation_products.groupby(["yearweek"])["variation_id"]
            .count()
            .reset_index()
            .rename(columns={"variation_id": "default_dishes_left"})
        )

        # Count dishes left in pool
        selector_pool_status = (
            start_pool.groupby(["yearweek"])["variation_id"]
            .count()
            .reset_index()
            .rename(columns={"variation_id": "pool_dishes_left"})
        )

        # Merge new dishes left columns to selector deviation
        self.selector_deviations = self.selector_deviations.merge(
            selector_deviation_status, how="left", on=["yearweek"]
        )
        self.selector_deviations = self.selector_deviations.merge(
            selector_pool_status, how="left", on=["yearweek"]
        )

        self.selector_deviations.loc[:, ["default_dishes_left", "pool_dishes_left"]] = (
            self.selector_deviations.loc[:, ["default_dishes_left", "pool_dishes_left"]]
            .fillna(0)
            .astype(int)
        )

        # Add only rows where needed (only when > 1 dish is filtered away)
        self.add_rows_where_dishes_removed(start_deviation_products, start_pool)

    def add_rows_where_dishes_removed(self, start_deviation_products, start_pool):
        """Add only rows where needed (only when > 1 dish is filtered away)"""
        self.selector_deviation_products = self.selector_deviations.loc[
            self.selector_deviations["expected_dishes"]
            != self.selector_deviations["default_dishes_left"]
        ].merge(start_deviation_products, how="inner", on=["yearweek"])[
            ["yearweek", "variation_id", "main_recipe_ids"]  # "year", "week",
        ]

        self.selector_deviation_products["quantity"] = 1
        self.selector_pool = start_pool

        # Set 0 if no matches
        self.selector_deviations["max_dishes"] = (
            self.selector_deviations["default_dishes_left"]
            + self.selector_deviations["pool_dishes_left"]
        )
        self.selector_deviations["missing_dishes"] = (
            self.selector_deviations["expected_dishes"]
            - self.selector_deviations["default_dishes_left"]
        )
        self.selector_deviations["max_comb_size"] = self.selector_deviations[
            ["missing_dishes", "pool_dishes_left"]
        ].min(axis=1)

    def set_selector_result(self):
        """
        Sets result where not yet set and picks what financial to insert into selector_deviation_products
        """
        logger.debug("Set final selector results")

        iterable = zip(
            *self.selector_deviations[
                [
                    # "year",
                    # "week",
                    "yearweek",
                    "expected_dishes",
                    "max_dishes",
                    "default_dishes_left",
                ]
            ].T.values
        )

        for (
            # year,
            # week,
            yearweek,
            expected_dishes,
            max_dishes,
            default_dishes_left,
        ) in iterable:
            if expected_dishes == 0:
                self.set_selector_deviations_status(yearweek, "failed")  # year, week
                logger.error(
                    "Failed to generate deviations with expected_dishes == 0. Agreement_id %s",
                    str(self.basket["agreement_id"]),
                )
                logger.error(self.selector_deviations)

            elif max_dishes < expected_dishes:
                logger.debug(
                    "Max dishes less than expected; not enough dishes to populate."
                )
                seek_financial = self.get_alternative_financial(meals=max_dishes)
                if seek_financial:
                    logger.debug(
                        "Max dishes less than expected dishes, inserting financial"
                    )
                    self.set_selector_deviations_status(
                        yearweek, "not_enough_meals"
                    )  # year, week

                    self.selector_deviation_products = pd.concat(
                        [
                            self.selector_deviation_products,
                            pd.DataFrame(
                                {
                                    # "year": [year],
                                    # "week": [week],
                                    "yearweek": [yearweek],
                                    "variation_id": [seek_financial],
                                    "quantity": [1],
                                }
                            ),
                        ],
                        ignore_index=True,
                    )

                else:
                    self.set_selector_deviations_status(
                        yearweek, "failed"
                    )  # year, week

                    # Remove dishes from deviation
                    self.selector_deviation_products = (
                        self.selector_deviation_products.loc[
                            ~(self.selector_deviation_products["yearweek"] == yearweek)
                        ]
                    )
                    logger.error(
                        "Failed to generate deviations with no financial alternative. Agreement_id %s",
                        str(self.basket["agreement_id"]),
                    )

            elif default_dishes_left == expected_dishes:
                logger.debug("Default dishes left equal to expected dishes")
                self.set_selector_deviations_status(yearweek, "default")  # year, week

            else:
                logger.debug("Max dishes greater than expected dishes")
                self.set_selector_deviations_status(yearweek, "success")  # year, week

                # Inserting financial
                self.selector_deviation_products = pd.concat(
                    [
                        self.selector_deviation_products,
                        pd.DataFrame(
                            {
                                # "year": [year],
                                # "week": [week],
                                "yearweek": [yearweek],
                                "variation_id": [
                                    self.basket_product["financial_variation_id"]
                                ],
                                "quantity": [1],
                            }
                        ),
                    ],
                    ignore_index=True,
                )

    def set_selector_deviations_status(self, yearweek, status):
        """Set the status of selector deviation for a specific year and week"""
        status_mapper = {
            "success": constants.SUCCESS,
            "default": constants.NO_CHANGES_IN_BASKET,
            "not_enough_meals": constants.NOT_ENOUGH_MEALS_OUTPUT,
            "failed": constants.FAILED,
        }

        self.selector_deviations.loc[
            # (self.selector_deviations["year"] == year)
            # & (self.selector_deviations["week"] == week),
            self.selector_deviations["yearweek"] == yearweek,
            "result",
        ] = status_mapper[status]

    def add_score_to_selector_pool(self, batch="selector"):
        """Adds recommendation scores to the dishes in pool

        Args:
            dish_pool (DataFrame): DataFrame with dishes that are suitable to be inserted in the basket
            week_year (Dict): Basket's week and year
            agreement_id (Guid): Agreement's unique identifier

        Returns:
            df: Pool of suitable dishes with recommendation scores
        """
        logger.debug("Adding recommendation scores to dishes in pool")
        weeks_to_fetch = self.selector_deviations.loc[
            self.selector_deviations["result"].isin(
                (constants.SUCCESS, constants.NOT_ENOUGH_MEALS_OUTPUT)
            ),
            ["yearweek"],  # 'year', 'week'
        ].to_dict(orient="records")

        if batch == "batch_selector":
            recs = pm.get_recs_agreement_batch(
                self.basket["agreement_id"], weeks_to_fetch, self.basket["company_id"]
            )
        else:
            recs = pm.get_recs_agreement(
                self.basket["agreement_id"], weeks_to_fetch, self.basket["company_id"]
            )

        # Making recommendations not mandatory
        if recs.empty:
            logger.debug(
                "Recommendations are empty, setting default order of relevance"
            )
            self.selector_pool.reset_index(drop=True)
            self.selector_pool["order_of_relevance"] = (
                self.selector_pool.groupby(["yearweek"]).cumcount() + 1
            )
        else:
            self.selector_pool = self.selector_pool.merge(
                recs, how="left", on=["yearweek", "product_id"]
            )
            if self.selector_pool["order_of_relevance"].isnull().values.any():
                self.selector_pool.sort_values(
                    by=["yearweek", "order_of_relevance"],
                    ignore_index=False,
                    na_position="last",
                    inplace=True,
                )
                self.selector_pool = self.selector_pool.reset_index(
                    drop=True
                )  # Do we need this?
                self.selector_pool["order_of_relevance"] = (
                    self.selector_pool.groupby(["yearweek"]).cumcount() + 1
                )

    def add_basket_other_products_to_selector_deviation(self):
        """Adds the other products to the necessary deviations"""
        logger.debug("Adding other products to the necessary deviations")
        other_deviations = self.selector_deviations.loc[
            self.selector_deviations["result"].isin(
                (constants.SUCCESS, constants.NOT_ENOUGH_MEALS_OUTPUT)
            ),
            ["yearweek"],  # 'year', 'week'
        ]
        if other_deviations.empty:
            logger.debug("Other deviations empty")
        else:
            other_deviations["key"] = 1
            self.basket_other_products["key"] = 1

            other_deviation_products = other_deviations.merge(
                self.basket_other_products, how="inner", on=["key"]
            ).drop(columns="key")
            self.basket_other_products.drop(columns="key", inplace=True)

            self.selector_deviation_products = pd.concat(
                [self.selector_deviation_products, other_deviation_products]
            )

    def filter_selector_deviation(self):
        """Filters the selector deviation on results"""

        self.accepted_deviations = self.selector_deviations.loc[
            self.selector_deviations["result"].isin(
                (constants.SUCCESS, constants.NOT_ENOUGH_MEALS_OUTPUT)
            )
        ]
