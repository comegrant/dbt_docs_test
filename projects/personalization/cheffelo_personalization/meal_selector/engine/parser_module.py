# Holds the dataprep logic.
import json
import os

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from cheffelo_personalization.meal_selector.utils import config, constants
from cheffelo_personalization.utils.log import log_handler

logger, _ = log_handler.initialize_logging(
    os.environ.get("LOG_LEVEL", "DEBUG"), __name__
)


def open_json(path):
    with open(path) as f:
        data = json.load(f)

    return data


def parse_data(
    cms=None,
    psl=None,
    pim=None,
    preselected=None,
    psl_parsed: pd.DataFrame = pd.DataFrame(),
    pim_parsed: pd.DataFrame = pd.DataFrame(),
    preselected_parsed: pd.DataFrame = pd.DataFrame(),
):
    """Takes the input data and parse it before passing it to the filter"""
    cms_parsed = parse_cms_data(cms)
    weeks = cms_parsed["weeks"]

    # Check whether there is data data already parsed
    psl_parsed = parse_psl_data(weeks, psl) if psl_parsed.empty else psl_parsed

    pim_parsed = parse_pim_data(weeks, pim) if pim_parsed.empty else pim_parsed

    preselected_parsed = (
        parse_pim_preselected_data(weeks, preselected)
        if preselected_parsed.empty
        else preselected_parsed
    )

    logger.debug("Parsed data on Filter")
    try:
        assert not psl_parsed.empty
        assert not pim_parsed.empty
        assert not preselected_parsed.empty
    except AssertionError as err:
        logger.exception(err)
        logger.warning("Empty data from PSL/PIM.")

    return cms_parsed, psl_parsed, pim_parsed, preselected_parsed, weeks


def get_recs_agreement(agreement_id, week_year, company_id):
    """Gets recommendations for a specific agreement_id and a set of year/weeks

    Args:
        agreement_id (int): Agreement's uniqueidentifier
        week_year (List): List of dictionaries with weeks/years
        company_id (str): Company's uniqueidentifier.

    Returns:
        [type]: [description]
    """
    list_week_years = []
    for i in week_year:
        list_week_years.append(str(i["yearweek"]))
        # list_week_years.append(str(i["year"] * 100 + i["week"]))

    if len(list_week_years) == 0:
        return pd.DataFrame(
            columns=["yearweek", "product_id", "order_of_relevance"]  # yearweek
        )

    str_week_years = "(" + "), (".join(list_week_years) + ")"
    read_db = DB(db="ml")
    query = f"""
        SET NOCOUNT ON;
        DECLARE @yearweeks yearweeklist;
        INSERT INTO @yearweeks VALUES {str_week_years};
        EXEC py_analytics.usp_GetAgreementPreferenceRecommendations
            @agreement_id = {agreement_id},
            @company_id = '{company_id}',
            @yearweeks = @yearweeks
    """
    df_agr = pd.read_sql_query(query, read_db.get_engine())

    # Ensure that the ordering from the db is complete
    df_agr.sort_values(by=["year", "week", "order_of_relevance"], inplace=True)
    df_agr["order_of_relevance"] = df_agr.groupby(["year", "week"]).cumcount() + 1
    df_agr["product_id"] = df_agr.loc[:, "product_id"].str.lower()
    df_agr["yearweek"] = df_agr["year"] * 100 + df_agr["week"]

    # If rec engine has duplicates, remove them and keep first
    if len(df_agr["product_id"].duplicated()) > 0:
        df_agr = df_agr.drop_duplicates(subset="product_id", keep="first")
        logger.warning("Recommendation score has duplicated product id!")

    df_agr.drop(["year", "week"], axis=1, inplace=True)

    return df_agr


def get_recs_agreement_batch(agreement_id, week_year, company_id):
    """Gets recommendations for a specific agreement_id and a set of year/weeks from batch_quarantine

    Args:
        agreement_id (int): Agreement's uniqueidentifier
        week_year (int): Week from data to be fetch
        company_id (str): Company's uniqueidentifier.

    Returns:
        [type]: [description]
    """

    if len(week_year) == 0:
        return pd.DataFrame(
            columns=["yearweek", "product_id", "order_of_relevance"]  # yearweek
        )

    year = week_year[0]["yearweek"] // 100
    week = week_year[0]["yearweek"] % 100

    query = f"""
            SELECT *
            FROM [py_analytics].[batch_preference_quarantine]
            WHERE agreement_id = {agreement_id}
            AND company_id = '{company_id}'
            AND recommended_week = {week}
            AND year = {year}
        """

    read_db = DB("ml")

    df_agr = pd.read_sql_query(query, read_db.get_engine())
    df_agr.rename(
        columns={
            "recommended_week": "week",
            "order_of_relevance_cluster": "order_of_relevance",
        },
        inplace=True,
    )
    # Ensure that the ordering from the db is complete
    df_agr.sort_values(by=["year", "week", "order_of_relevance"], inplace=True)
    df_agr["order_of_relevance"] = df_agr.groupby(["year", "week"]).cumcount() + 1
    df_agr["product_id"] = df_agr.loc[:, "product_id"].str.lower()
    df_agr["yearweek"] = df_agr["year"] * 100 + df_agr["week"]

    # If rec engine has duplicates, remove them and keep first
    if len(df_agr["product_id"].duplicated()) > 0:
        df_agr = df_agr.drop_duplicates(subset="product_id", keep="first")
        logger.warning("Recommendation score has duplicated product id!")

    return df_agr


def parse_psl_data(weeks, weekly_pool=None):
    """Parses PSL Data for a list of week dicts"""
    if not weekly_pool:
        logger.warning("No psl data detected, reading from local")
        weekly_pool = open_json(config.PROCESSED_DATA_DIR / "data-psl.json")

    psl_fields = [
        "product_id",
        "product_type_id",
        "variation_id",
        "name",
        "flex_financial_variation_id",
        "portions",
        "meals",
        "price",
    ]

    products = pd.DataFrame(columns=["yearweek"] + psl_fields)

    list_products = [products]
    for psl_week in weekly_pool:
        if len(psl_week) == 0:
            logger.error("PSL data is empty!")
            continue

        current_psl_week = {
            "week": int(psl_week["week"]),
            "year": int(psl_week["year"]),
        }
        if current_psl_week in weeks:
            week_products = pd.DataFrame(psl_week["products"])
            # week_products["year"] = int(current_psl_week["year"])
            # week_products["week"] = int(current_psl_week["week"])
            week_products["yearweek"] = (
                current_psl_week["year"] * 100 + current_psl_week["week"]
            )
            list_products.append(week_products)

    products = pd.concat(list_products)

    if len(products) == 0:
        logger.error("Length of psl data is 0")

    return products


def parse_pim_data(weeks, weekly_pool=None):
    """Parses PIM Data for a list of week dicts"""

    if not weekly_pool:
        logger.warning("No pim data detected, reading from local")
        weekly_pool = open_json(config.PROCESSED_DATA_DIR / "data-pim.json")

    recipes = pd.DataFrame(
        columns=[
            "yearweek",
            "product_id",
            "variation_id",
            "preferences",
            "portions",
            "main_recipe_ids",
        ]
    )

    list_recipes = [recipes]
    for pim_week in weekly_pool:
        # Check if the current week is empty
        if len(pim_week) == 0:
            logger.error("Pim data is empty!")
            continue

        current_pim_week = {
            "week": int(pim_week["week"]),
            "year": int(pim_week["year"]),
        }
        if current_pim_week in weeks:
            for portions in pim_week["recipes_by_portion"]:
                week_recipes = pd.DataFrame(portions["recipes"])
                week_recipes["portions"] = portions["portion"]
                week_recipes["yearweek"] = (
                    current_pim_week["year"] * 100 + current_pim_week["week"]
                )
                list_recipes.append(week_recipes)

    recipes = pd.concat(list_recipes)

    if len(recipes) == 0:
        logger.error("Length of pim data is 0")

    return recipes


def parse_pim_preselected_data(weeks, weekly_preselected=None):
    """
    Parse the PIM preselected data
    """
    if not weekly_preselected:
        logger.warning("No weekly preselected data detected, loading from local")
        weekly_preselected = open_json(
            config.PROCESSED_DATA_DIR / "data-pim-preselected.json"
        )

    preselected = pd.DataFrame(
        columns=[
            "yearweek",
            "product_id",
            "preselected_product_id",
            "recipe_order",
        ]
    )
    list_weeks = [preselected]
    for pre_week in weekly_preselected:
        # Check if the current week is empty
        if len(pre_week) == 0:
            logger.error("Preselected pim data is empty for a week!")
            continue

        current_week = {"week": int(pre_week["week"]), "year": int(pre_week["year"])}
        if current_week in weeks:
            for products in pre_week["products"]:
                week_preselected = pd.DataFrame(products["preselected_products"])
                week_preselected = week_preselected.loc[
                    :, ["product_id", "recipe_order"]
                ]
                week_preselected.rename(
                    columns={"product_id": "preselected_product_id"}, inplace=True
                )
                week_preselected["product_id"] = str(products["product_id"])
                week_preselected["yearweek"] = (
                    current_week["year"] * 100 + current_week["week"]
                )
                list_weeks.append(week_preselected)
    preselected = pd.concat(list_weeks)

    if len(preselected) == 0:
        logger.warning("Length of preselected pim data is 0")

    return preselected


def parse_rules_for_product(product_id, ruleset=None):
    """
    Parses rules for a product_id.
    The rule engine will run a function filtering down the set of rules
    (depending on what rules we're able to apply for the dishes)
    """

    if ruleset:
        logger.debug("Using DB ruleset")
        full_product_rules = ruleset.get(product_id, {}).get("rules", [])
        if not full_product_rules:
            logger.warning("No ruleset for given product_id: %s", product_id)
    else:
        logger.warning("No ruleset detected, loading from local")
        rules = open_json(config.PROCESSED_DATA_DIR / "rules_suggestion.json")
        full_product_rules = []
        if product_id in rules.keys():
            product_rules = rules[product_id]["rules"]
            for r in product_rules:
                r["type"] = constants.RULE_TYPES[r["rule_type_id"]]["type"]
                r["rule_type"] = constants.RULE_TYPES[r["rule_type_id"]]["name"]
                full_product_rules.append(r)

    return full_product_rules


def parse_cms_data(cms_input=None):
    """Handles one agreement at a time"""
    if cms_input is None:
        cms_input = open_json(config.PROCESSED_DATA_DIR / "data-cms.json")

    preferences = {
        constants.TASTE_PREFERENCE: set(),
        constants.CONCEPT_PREFERENCES: set(),
    }

    for preference in cms_input["preferences"]:
        if preference["preference_type_id"] == constants.TASTE_PREFERENCE:
            preferences[constants.TASTE_PREFERENCE].add(preference["preference_id"])
        if preference["preference_type_id"] == constants.CONCEPT_PREFERENCES:
            preferences[constants.CONCEPT_PREFERENCES].add(preference["preference_id"])

    return {
        "weeks": cms_input["weeks"],
        "company_id": cms_input["company_id"],
        "agreement_id": cms_input["agreement_id"],
        "products": cms_input["products"],
        "preferences": preferences,
    }


def get_output(filtered) -> dict:
    """
    Takes in a filtered object and returns the dict to send back from analytics
    Should create a dict of the following structure (one element for every week):

    {
        "agreement_id": 123,
        "weeks": [
            {
                "year": 2021,
                "week": 20,
                "result": "SUCCESS",
                "products": [
                    {
                        'variation_id': '2517fe8a-e4a0-4744-8763-e804e408b886'
                        'quantity': 1
                    }
                ]
            }, ...
        ]
    }
    """
    # "year", "week"
    all_data = filtered.selector_deviations.loc[:, ["yearweek", "result"]].merge(
        filtered.selector_deviation_products, how="left", on=["yearweek"]
    )

    all_data["products"] = [
        [{"variation_id": variation_id, "quantity": int(quantity)}]
        if pd.notna(quantity)
        else []
        for variation_id, quantity in zip(
            all_data["variation_id"], all_data["quantity"]
        )
    ]
    all_data["week"] = all_data["yearweek"].apply(lambda x: int(str(x)[-2:]))
    all_data["year"] = all_data["yearweek"].apply(lambda x: int(str(x)[:4]))

    weeks = (
        all_data.groupby(["year", "week", "result"])["products"]
        .sum()
        .reset_index()
        .to_dict(orient="records")
    )

    return {"agreement_id": filtered.basket["agreement_id"], "weeks": weeks}


def get_error_output(cms) -> dict:
    """
    If the model throws an exception, the following dict will be returned

    {
        "agreement_id": 123,
        "weeks": [
            {
                "year": 2021,
                "week": 20,
                "result": "FAILED",
                "products": []
            }, ...
        ]
    }
    """
    error_output = [
        {"week": week["week"], "year": week["year"], "products": [], "result": "FAILED"}
        for week in cms["weeks"]
    ]

    return {"agreement_id": cms["agreement_id"], "weeks": error_output}
