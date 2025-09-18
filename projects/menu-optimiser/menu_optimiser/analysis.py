import pandas as pd
from menu_optimiser.common import BusinessRule
from menu_optimiser.config import seasonal_taxonomies


def get_needed_recipe_ids(
    available_recipes_df: pd.DataFrame,
    recipe_universe_df: pd.DataFrame,
    taxonomy_constraints: dict[int, BusinessRule],
) -> tuple[list[int], list[str]]:
    """Extract recipe IDs needed to fill gaps in taxonomy constraints, excluding recipes with seasonal taxonomies."""
    all_needed_ids = set()
    msg = []

    for taxonomy, business_rule in taxonomy_constraints.items():
        available_df = pd.DataFrame(
            available_recipes_df[
                available_recipes_df["taxonomies"].apply(lambda x: taxonomy in x)
            ]
        )
        universe_df = pd.DataFrame(
            recipe_universe_df[
                recipe_universe_df["taxonomies"].apply(lambda x: taxonomy in x)
            ]
        )

        # exclude recipes with any seasonal taxonomy from universe_df
        universe_df = universe_df[
            ~universe_df["taxonomies"].apply(
                lambda taxonomies: any(tax in seasonal_taxonomies for tax in taxonomies)
            )
        ]

        # iterate over each constraint type
        for column, constraints in [
            ("main_ingredient_id", business_rule.main_ingredients),
            ("price_category_id", business_rule.price_category),
            ("cooking_time_to", business_rule.cooking_time_to),
        ]:
            for key, required_amount in constraints.items():
                available_count = len(available_df[available_df[column] == key])

                if available_count < required_amount:
                    msg.append(
                        f"Found {available_count}/{required_amount} recipes for {column} {key} with taxonomy {taxonomy}"
                    )
                    universe_recipes = universe_df[universe_df[column] == key]
                    universe_recipe_ids = set(universe_recipes["recipe_id"])

                    gap = required_amount - available_count
                    all_needed_ids.update(
                        list(universe_recipe_ids)[: gap * 2]
                    )  # 2x for safety buffer

    return list(all_needed_ids), msg


def adjust_available_recipes(
    available_recipe_df: pd.DataFrame,
    required_recipe_df: pd.DataFrame,
    recipe_universe_df: pd.DataFrame,
    taxonomy_constraints: dict[int, BusinessRule],
) -> tuple[pd.DataFrame, str]:
    """Supplement with recipes from quarantine to meet taxonomy constraints."""

    available_recipes = available_recipe_df["recipe_id"].tolist()
    required_recipes = required_recipe_df["recipe_id"].tolist()

    quarantine_df = pd.DataFrame(
        recipe_universe_df[
            ~recipe_universe_df["recipe_id"].isin(available_recipes + required_recipes)
        ]
    )

    needed_recipe_ids, msg = get_needed_recipe_ids(
        available_recipe_df, quarantine_df, taxonomy_constraints
    )

    new_recipe_ids = [rid for rid in needed_recipe_ids if rid not in available_recipes]

    if new_recipe_ids:
        extra_recipes_df = recipe_universe_df[
            recipe_universe_df["recipe_id"].isin(new_recipe_ids)
        ]
        adjusted_df = pd.concat(
            [available_recipe_df, extra_recipes_df], ignore_index=True
        )
        msg.append(
            f"Added {len(new_recipe_ids)} recipes from quarantine: {', '.join(map(str, new_recipe_ids))}."
        )
    else:
        adjusted_df = available_recipe_df
        msg.append("No additional recipes needed.")

    return pd.DataFrame(adjusted_df), ". ".join(msg)
