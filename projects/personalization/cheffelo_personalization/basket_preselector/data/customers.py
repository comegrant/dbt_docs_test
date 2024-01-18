import logging
import sys
from datetime import date, timedelta

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from cheffelo_personalization.basket_preselector.utils.constants import (
    CONCEPT_PREFERENCE_TYPE_ID,
    MEALBOX_PRODUCT_TYPE_ID,
    TASTE_PREFERENCE_TYPE_ID,
)
from cheffelo_personalization.basket_preselector.utils.paths import SQL_CUSTOMER_DIR

logger = logging.getLogger(__name__)


class CustomerData:
    def __init__(self, company_id: str, db: DB):
        self.company_id = company_id
        self.db = db

    def _read_data_from_db(self, query_file: str, **args):
        """Reaad data from database

        Args:
            query_file (str): _description_

        Returns:
            _type_: _description_
        """
        with open(SQL_CUSTOMER_DIR / query_file) as f:
            return self.db.read_data(f.read().format(**args))

    def get_customer_information_for_agreement_id(
        self, agreement_id: str
    ) -> pd.DataFrame:
        """Get customer information for one agreement id

        Args:
            agreement_id (str): agreement id of customer
            read_db (DB): database conneciton

        Returns:
            pd.DataFrame: dataframe of customer
        """
        logger.info("Reading customer information for agreement id %s", agreement_id)
        return self._read_data_from_db(
            query_file="get_customer_information.sql", agreement_id=agreement_id
        )

    def get_customer_information_for_agreement_ids(
        self, agreement_ids: list
    ) -> pd.DataFrame:
        """Gets customer information for a list of agreement ids

        Args:
            agreement_ids (list): list of agreement ids to get customer information from
            read_db (DB): database connection

        Returns:
            pd.DataFrame: dataframe of customers
        """
        logger.info("Reading customer information in batch")
        df_customers = self._read_data_from_db(
            query_file="get_customer_information_batch.sql", agreement_ids=agreement_ids
        )

        if df_customers is None:
            logger.exception("No customers!")
            sys.exit(0)

        logger.info("Fetched %s customers", len(df_customers))

        return df_customers

    def get_customer_information_for_company_id(
        self, status: str = "10"
    ) -> pd.DataFrame:
        """Gets all active customers for a given company id

        Args:
            company_id (str): id of company to fetch active agreement ids
            read_db (DB): database connection

        Returns:
            DataFrame: pandas dataframe of customer information
        """
        logger.info(
            "Reading customer information in batch for company %s", self.company_id
        )
        df_customers_information = self._read_data_from_db(
            query_file="get_customer_information_for_company_id.sql",
            status=status,
            company_id=self.company_id,
            concept_preference_type_id=CONCEPT_PREFERENCE_TYPE_ID,
            taste_preference_type_id=TASTE_PREFERENCE_TYPE_ID,
            product_type_id_mealbox=MEALBOX_PRODUCT_TYPE_ID,
        )

        if df_customers_information is None:
            logger.exception("No customers!")
            sys.exit(0)

        logger.info("Fetched %s customers", len(df_customers_information))

        df_customers_information = df_customers_information.dropna(
            subset=["taste_preference_ids", "concept_preference_ids"], how="all"
        )
        return df_customers_information

    def get_customers_deviations_for_week(self, year: int, week: int) -> pd.DataFrame:
        logger.info("Reading customers deviations for week %s", week)
        df_customer_deviations = self._read_data_from_db(
            query_file="get_customer_deviations.sql",
            company_id=self.company_id,
            year=year,
            week=week,
        )

        logger.info(
            "Fetched %s customers with deviations",
            len(df_customer_deviations["agreement_id"].unique()),
        )

        return df_customer_deviations

    def get_customers_deviations_for_weeks(
        self, start_year: int, start_week: int, num_weeks: int
    ) -> pd.DataFrame:
        """Fetches the deviations for last n weeks given a start date

        Args:
            start_year (int): start date year
            start_week (int): start date week
            num_weeks (int): how many weeks in the past from start week we want to fetch deviations from
            db (DB): database object

        Returns:
            pd.DataFrame: dataframe of customer deviations for a number of weeks
        """
        yearweekdate = date.fromisocalendar(start_year, start_week, 1)
        daterange = [yearweekdate - timedelta(weeks=x) for x in range(1, num_weeks + 1)]
        deviations = []
        for yearweek in daterange:
            deviations.append(
                self.get_customers_deviations_for_week(
                    year=yearweek.year,
                    week=yearweek.isocalendar()[1],
                )
            )

        return pd.concat(deviations)

    def get_customers_past_orders_for_week(self, year: int, week: int) -> pd.DataFrame:
        logger.info("Get customers past orders for week %s and year %s", week, year)
        df_customer_deviations = self._read_data_from_db(
            query_file="get_customer_past_orders.sql",
            company_id=self.company_id,
            year=year,
            week=week,
        )

        logger.info(
            "Fetched %s customers with past orders",
            len(df_customer_deviations["agreement_id"].unique()),
        )

        return df_customer_deviations

    def get_customers_past_orders_for_weeks(
        self, start_year: int, start_week: int, num_weeks: int
    ) -> pd.DataFrame:
        """Fetches the deviations for last n weeks given a start date

        Args:
            start_year (int): start date year
            start_week (int): start date week
            num_weeks (int): how many weeks in the past from start week we want to fetch deviations from
            db (DB): database object

        Returns:
            pd.DataFrame: dataframe of customer deviations for a number of weeks
        """
        yearweekdate = date.fromisocalendar(start_year, start_week, 1)
        daterange = [yearweekdate - timedelta(weeks=x) for x in range(1, num_weeks + 1)]
        past_order_weeks = []
        for yearweek in daterange:
            past_order_weeks.append(
                self.get_customers_past_orders_for_week(
                    year=yearweek.year,
                    week=yearweek.isocalendar()[1],
                )
            )

        return pd.concat(past_order_weeks)

    def get_customers_default_dishes_for_week(
        self, year: int, week: int
    ) -> pd.DataFrame:
        logger.info("Get customers past orders for week %s and year %s", week, year)
        df_customer_default_dishes = self._read_data_from_db(
            query_file="get_customer_default_basket_for_concept.sql",
            company_id=self.company_id,
            year=year,
            week=week,
        )

        logger.info(
            "Fetched %s customers with default dishes",
            len(df_customer_default_dishes["agreement_id"].unique()),
        )

        return df_customer_default_dishes

    def get_customers_default_dishes_for_weeks(
        self, start_year: int, start_week: int, num_weeks: int
    ) -> pd.DataFrame:
        """Fetches the deviations for last n weeks given a start date

        Args:
            start_year (int): start date year
            start_week (int): start date week
            num_weeks (int): how many weeks in the past from start week we want to fetch deviations from
            db (DB): database object

        Returns:
            pd.DataFrame: dataframe of customer deviations for a number of weeks
        """
        yearweekdate = date.fromisocalendar(start_year, start_week, 1)
        daterange = [yearweekdate - timedelta(weeks=x) for x in range(1, num_weeks + 1)]
        default_dishes_weeks = []
        for yearweek in daterange:
            default_dishes_weeks.append(
                self.get_customers_default_dishes_for_week(
                    year=yearweek.year,
                    week=yearweek.isocalendar()[1],
                )
            )

        return pd.concat(default_dishes_weeks)

    def get_customer_quarantined_dishes(
        self, start_year: int, start_week: int, num_quarantine_weeks: int
    ) -> pd.DataFrame:
        """Generate the quarantined dishes from past orders and default and deviated dishes in planned orders

        Args:
            start_year (int): start year when we are fetching dishes
            start_week (int): start week when we are fetching dishes
            quarantine_period (int): how many weeks should the dishes be quarantined

        Returns:
            pd.DataFrame: a dataframe of main recipe ids that should be quarantined
        """
        # Get past orders
        df_customers_past_orders = self.get_customers_past_orders_for_weeks(
            num_weeks=num_quarantine_weeks, start_year=start_year, start_week=start_week
        ).set_index(["agreement_id", "year", "week"])
        df_customers_past_orders["source"] = "default_dishes"

        # Get default dishes
        df_customers_default_dishes = self.get_customers_default_dishes_for_weeks(
            num_weeks=num_quarantine_weeks, start_year=start_year, start_week=start_week
        ).set_index(["agreement_id", "year", "week"])
        df_customers_default_dishes["source"] = "default_dishes"

        # Get deviations from default dishes
        df_customers_deviations = self.get_customers_deviations_for_weeks(
            start_year=start_year,
            start_week=start_week,
            num_weeks=num_quarantine_weeks,
        ).set_index(["agreement_id", "year", "week"])
        df_customers_deviations["source"] = "default_dishes"

        # Set quarantined dishes as default dishes
        df_quarantine_dishes = df_customers_default_dishes
        df_quarantine_dishes = df_quarantine_dishes[["main_recipe_id", "source"]]

        # Add dishes from past orders if exist
        if not df_customers_past_orders.empty:
            logger.info("past orders not empty, adding to quarantined dishes")
            df_customers_past_orders = df_customers_past_orders[
                ["main_recipe_id", "source"]
            ]
            df_quarantine_dishes = pd.concat(
                [df_quarantine_dishes, df_customers_past_orders]
            )

        # If customer has deviated from default dishes, add these to the dishes in quarantine
        if not df_customers_deviations.empty:
            logger.info("agreement deviation not empty, adding to quarantined dishes")
            df_customers_deviations = df_customers_deviations[
                ["main_recipe_id", "source"]
            ]
            df_quarantine_dishes = pd.concat(
                [df_quarantine_dishes, df_customers_deviations]
            )

        return df_quarantine_dishes.reset_index()

    def get_customers_orders(
        self, num_weeks: int, year: int, week: int
    ) -> pd.DataFrame:
        logger.info("Get customers orders from week %s", week)


def get_customers_with_mealbox(
    df_customers: pd.DataFrame, df_mealboxes: pd.DataFrame
) -> pd.DataFrame:
    """Find all customers subscribed to a mealbox

    Args:
        df_customers (pd.DataFrame): customers
        df_mealboxes (pd.DataFrame): mealboxes

    Returns:
        pd.DataFrame: customers subscribed to a mealbox
    """
    df_customers = df_customers[
        df_customers["subscribed_product_variation_id"].isin(
            df_mealboxes["variation_id"]
        )
    ].reset_index(drop=True)

    # Merge in subscription information, such as portion, meals and price
    df_customers = df_customers.merge(
        df_mealboxes, right_on="variation_id", left_on="subscribed_product_variation_id"
    )

    df_customers = df_customers.assign(
        taste_preference_ids=df_customers["taste_preference_ids"].apply(
            lambda x: x.lower().split(", ") if isinstance(x, str) else []
        )
    )

    df_customers = df_customers.astype({"variation_meals": int})

    return df_customers
