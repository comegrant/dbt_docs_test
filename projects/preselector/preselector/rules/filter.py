"""
Contains the soft rules to choose the preselected dishes, with soft rules we do not remove dishes, only modify their score.

Types of hard rules:
- Chef defining rules, contains the rules defined by chefs, for example minimum one dish of potatoes and maximum 3
- Quarantine rules, dishes that are already selected in earlier weeks for this customer should have lower priority
"""
import logging

import pandas as pd

logger = logging.getLogger()


def has_taste_restrictions(customer: pd.DataFrame) -> bool:
    """Check if customer has taste restrictions"""
    if customer["taste_preference_ids"] is None or customer["taste_preference_ids"] == []:
        return False
    return True


def list_contains_preference_value(column, preference_restrictions):  # noqa: ANN001
    """Checks if any of the value in column exist in the unwanted recipe preferences"""
    return any(value in preference_restrictions for value in column)


def filter_taste_restrictions(
    possible_dishes: pd.DataFrame,
    taste_preference_ids: list[str],
    debug_summary: dict,
) -> tuple[pd.DataFrame, dict] | Exception:
    """Filters out dishes that contains unwanted preferences"""
    if not taste_preference_ids:
        debug_summary["filter_taste_restrictions"] = "No taste restrictions"
        return possible_dishes, debug_summary

    # Assign dishes that has unwanted preferences
    possible_dishes = possible_dishes.assign(
        has_unwanted_preference=possible_dishes["preference_ids"].apply(
            lambda x: list_contains_preference_value(x, taste_preference_ids),
        ),
    )

    possible_dishes_before = len(possible_dishes)

    # Filter dishes with unwanted preferences
    possible_dishes = possible_dishes.loc[~possible_dishes["has_unwanted_preference"]]

    # Calculate dishes removed
    possible_dishes_after_filtering = len(possible_dishes)
    total_dishes_removed = possible_dishes_before - possible_dishes_after_filtering

    debug_summary["filter_taste_restrictions"] = (
        f"Removed {total_dishes_removed} dishes due to taste "
        f"restrictions ({possible_dishes_before} to {possible_dishes_after_filtering})"
    )

    if possible_dishes_after_filtering == 0:
        return ValueError(
            f"No possible dishes for customer after filtering taste restrictions: {taste_preference_ids}",
        )

    return possible_dishes, debug_summary


def filter_portion_size(
    possible_dishes: pd.DataFrame,
    portion_size: int,
    debug_summary: dict,
) -> tuple[pd.DataFrame, dict] | Exception:
    """Filter out dish variation with correct portion size"""
    total_dishes_before_filtering = len(possible_dishes)
    possible_dishes = possible_dishes[possible_dishes["variation_portions"].astype(int) == portion_size]

    # Calculate how many dishes removed
    total_dishes_after_filtering = len(possible_dishes)
    total_dishes_removed = total_dishes_before_filtering - total_dishes_after_filtering

    debug_summary[
        "filter_portion_sizes"
    ] = f"Removed {total_dishes_removed} dishes due to portion sizes ({total_dishes_before_filtering} to {total_dishes_after_filtering})"

    if total_dishes_after_filtering == 0:
        return ValueError(
            f"No possible dishes for customer after filtering portion sizes: {portion_size}",
        )

    return possible_dishes, debug_summary
