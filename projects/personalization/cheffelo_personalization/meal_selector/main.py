import os

import pandas as pd

import cheffelo_personalization.meal_selector.engine.parser_module as parser_module
from cheffelo_personalization.meal_selector.engine.filtering import Filter
from cheffelo_personalization.meal_selector.engine.rules_engine import RulesEngine
from cheffelo_personalization.meal_selector.utils import constants
from cheffelo_personalization.utils.log import log_handler

logger, main_logger = log_handler.initialize_logging(
    os.environ.get("LOG_LEVEL", "DEBUG"), __name__, "selector_main_logger"
)


def selector(
    cms=None,
    psl=None,
    pim=None,
    rules=None,
    preselected=None,
    psl_parsed: pd.DataFrame = pd.DataFrame(),
    pim_parsed: pd.DataFrame = pd.DataFrame(),
    preselected_parsed: pd.DataFrame = pd.DataFrame(),
    origin_log="selector",  # can be batch_selector as an Identifier
):
    """
    Run the mealselector job for one user
    """
    try:
        # Parse input data
        (
            cms_parsed,
            psl_parsed,
            pim_parsed,
            preselected_parsed,
            weeks,
        ) = parser_module.parse_data(
            cms, psl, pim, preselected, psl_parsed, pim_parsed, preselected_parsed
        )

        # Run filter process
        filtered = run_filter_process(
            cms_parsed, psl_parsed, pim_parsed, preselected_parsed, weeks, origin_log
        )

        # Run rule engine
        if has_rule_engine(filtered):
            run_rule_engine(filtered, rules)

        # Generate output
        output = parser_module.get_output(filtered)

        if not has_enough_meals(output):
            err_list = log_handler.build_log()
            main_logger.warning(
                "Success with not enough meals",
                extra={
                    "input": cms,
                    "output": output,
                    "origin": origin_log,
                    "errors": err_list,
                },
            )
        elif not all_weeks_successfull(output):
            err_list = log_handler.build_log()
            main_logger.warning(
                "Selector failed for one or more weeks",
                extra={
                    "input": cms,
                    "output": output,
                    "origin": origin_log,
                    "errors": err_list,
                },
            )
        elif has_duplicated_variation_ids(output):
            err_list = log_handler.build_log()
            main_logger.warning(
                "Selector returned with duplicated variations",
                extra={
                    "input": cms,
                    "output": output,
                    "origin": origin_log,
                    "errors": err_list,
                },
            )
        else:
            main_logger.info(
                "Success",
                extra={
                    "input": cms,
                    "output": output,
                    "origin": origin_log,
                },
            )
    except Exception as err:
        output = parser_module.get_error_output(cms)
        logger.exception(err)

        err_list = log_handler.build_log()
        main_logger.error(
            "Failed to run meal_selector.",
            extra={
                "input": cms,
                "output": output,
                "origin": origin_log,
                "errors": err_list,
            },
        )
        raise err
    finally:
        log_handler.clear_buffer()

    return output


def batch_selector(cms=None, psl=None, pim=None, rules=None, preselected=None):
    """
    Run the mealselector job for a group of users
    """
    output = []
    cms_agreement = {"weeks": cms["weeks"], "company_id": cms["company_id"]}
    pim_parsed = parser_module.parse_pim_data(cms["weeks"], pim)
    psl_parsed = parser_module.parse_psl_data(cms["weeks"], psl)
    preselected_parsed = parser_module.parse_pim_preselected_data(
        cms["weeks"], preselected
    )
    for agreement in cms["agreements"]:
        cms_agreement["agreement_id"] = agreement["agreement_id"]
        cms_agreement["products"] = agreement["products"]
        cms_agreement["preferences"] = agreement["preferences"]
        deviations = selector(
            cms=cms_agreement,
            rules=rules,
            psl_parsed=psl_parsed,
            pim_parsed=pim_parsed,
            preselected_parsed=preselected_parsed,
            origin_log="batch_selector",
        )
        output.append(deviations.copy())

    return output


def run_filter_process(cms, psl, pim, preselected, weeks, batch=False):
    """
    Takes the parsed data as input and runs the filtering process
    """
    filtered = Filter(cms, psl, pim, preselected, weeks)
    filtered.run_filtering_process(batch)
    return filtered


def has_duplicated_variation_ids(output) -> bool:
    """Checks if the output has duplicated variation id"""
    products = [w["products"] for w in output["weeks"]]
    for weekly_product in products:
        variations = [product["variation_id"] for product in weekly_product]
        if len(set(variations)) < len(variations):
            return True

    return False


def has_rule_engine(filtered) -> bool:
    """Checks if we should run the rule engine"""
    if filtered.accepted_deviations.empty:
        logger.debug("Not running rule engine as accepted_deviations is empty")
        return False
    return True


def run_rule_engine(filtered, rules):
    """Takes the deviation after rules and"""
    # Fetch the basket_product_id
    basket_product_id = filtered.basket_product["product_id"]

    # Fetch the specific rules for this product
    rules = parser_module.parse_rules_for_product(basket_product_id, rules)
    return RulesEngine(filtered, basket_product_id, rules).process_rules()


def has_enough_meals(output) -> bool:
    """
    Check if all of the weeks returned with enough meals
    """
    return (
        len(
            [
                r
                for r in output["weeks"]
                if r["result"] in [constants.NOT_ENOUGH_MEALS_OUTPUT]
            ]
        )
        == 0
    )


def all_weeks_successfull(output) -> bool:
    """
    Check if all weeks returned successfull
    """
    return len([r for r in output["weeks"] if r["result"] in [constants.FAILED]]) == 0


if __name__ == "__main__":
    print(selector())
