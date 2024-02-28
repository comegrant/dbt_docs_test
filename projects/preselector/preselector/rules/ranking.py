
import pandas as pd


def rank_dishes_based_on_quarantine(
    possible_dishes: pd.DataFrame,
    df_quarantined_dishes_for_customer: pd.DataFrame,
    debug_summary: dict,
    quarantine_penalization: int = 5,
) -> tuple[pd.DataFrame, dict]:
    """Rank dishes based on quarantine, add a penalty to dishes that are already getting delivered / has been delivered to customer in previous weeks

    Args:
        possible_dishes (pd.DataFrame): All possible dishes in menu
        df_quarantined_dishes_for_customer (pd.DataFrame): Dishes that are already getting delivered / has been delivered to customer
        debug_summary (Dict): Summary of log

    Returns:
        Tuple[pd.DataFrame, Dict]: _description_
    """
    # Compare basket history vs dishes in menu, dishes that the customer has recently ordered or will recieve in weeks before, rank to lower position

    # TODO: Implement with talks to recipes linking
    # One change from PIM, dishes can be linked together, same dish but small differences such in protein (recipes_linking).

    # Move dishes that has been bought last 3 weeks down 5 spots
    if df_quarantined_dishes_for_customer.empty:
        debug_summary["ranking_quarantine"] = "Dishes to be quarantined is empty"
        return possible_dishes, debug_summary

    num_dishes_to_be_penalized = len(
        possible_dishes.loc[
            possible_dishes["main_recipe_id"].isin(
                df_quarantined_dishes_for_customer["main_recipe_id"],
            )
        ],
    )
    num_possible_dishes = len(possible_dishes)
    debug_summary[
        "ranking_quarantine"
    ] = f"Out of {num_possible_dishes} possible dishes, {num_dishes_to_be_penalized} are penalized {quarantine_penalization} points"
    possible_dishes.loc[
        possible_dishes["main_recipe_id"].isin(
            df_quarantined_dishes_for_customer["main_recipe_id"],
        ),
        "score",
    ] += quarantine_penalization

    return possible_dishes, debug_summary


def rank_dishes_basked_on_rec_engine(
    possible_dishes: pd.DataFrame,
    df_rec: pd.DataFrame,
    debug_summary: dict,
) -> tuple[pd.DataFrame, dict]:
    """Rank dishes based on recommendation engine

    Args:
        possible_dishes (pd.DataFrame): All possible dishes in menu
        df_rec (pd.DataFrame): dataframe of recommendation scores
        debug_summary (Dict): Summary of log

    Returns:
        Tuple[pd.DataFrame, Dict]: possible dishes sorted by recommendation score and log summary
    """
    if df_rec.empty:
        debug_summary["ranking_rec_engine"] = "Rec engine score empty"
        return possible_dishes, debug_summary

    debug_summary["ranking_rec_engine"] = "Has rec engine score"
    possible_dishes_with_rec_engine = possible_dishes.merge(
        df_rec[["product_id", "order_of_relevance", "order_of_relevance_cluster"]],
        on="product_id",
        how="left",
    )
    possible_dishes_with_rec_engine["score"] = (
        possible_dishes_with_rec_engine["score"]
        + possible_dishes_with_rec_engine["order_of_relevance_cluster"]
    )
    debug_summary[
        "ranking_rec_engine"
    ] = "Possible dishes reordered based on rec engine score"

    return possible_dishes_with_rec_engine.sort_values(by="score"), debug_summary
