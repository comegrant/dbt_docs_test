from typing import Optional, Iterable

import pandas as pd


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the recipe data.

    - Cleans and standardizes the specified columns into sets
    - Cleans the 'recipe_name' by stripping whitespace, converting to lowercase, and splitting into set.
    """

    df["ingredient_list"] = df["generic_ingredient_name_list"]
    cols = [
        "recipe_main_ingredient_name_local",
        "taxonomy_name_list",
        "ingredient_list",
        "preference_name_combinations",
    ]
    for col in cols:
        df[col] = df[col].apply(
            lambda x: [item.strip().lower() for item in str(x).split(",")]
        )
        df[col] = df[col].apply(set)

    df["recipe_name"] = (
        df["recipe_name"].str.strip().str.lower().apply(lambda x: set(x.split()))
    )

    return df


def seo_tag_to_taxonomy_id(seo_category: pd.DataFrame) -> dict:
    """For a DataFrame of SEO information, return a dictionary of taxonomy names and taxonomy ids.

    Example Output:
        {
            'Pizza': 3334,
            'Burger': 3335,
        }
    """
    return seo_category.set_index("taxonomy_name_english").to_dict()["taxonomy_id"]


def reverse_keyword_mapping(mapping: dict) -> dict:
    """Reverse and flatten a nested keyword mapping.

    Example Input:
        {
            'taxonomy': {'Child Friendly': ['family']},
            'ingredient': {'Spicy': ['spicy', 'chili']}
        }

    Example Output:
        {
            'family': 'Child Friendly',
            'spicy': 'Spicy',
            'chili': 'Spicy',
        }
    """
    return {
        category: {
            keyword: pref
            for pref, keywords in category_data.items()
            for keyword in keywords
        }
        for category, category_data in mapping.items()
    }


def simple_match(items: Iterable, mapping: dict, add_to: Optional[set] = None) -> set:
    """Match items against a mapping and return corresponding values.

    Args:
        items: Collection of items to match.
        mapping: Dictionary with items as keys and corresponding values.
        add_to: Existing set to add matches to; creates a new set if None.

    Returns:
        set: A set of matched values from the mapping.
    """
    matched = add_to or set()
    for item in items:
        if item in mapping:
            matched.add(mapping[item])
    return matched


def extract_sorted_taxonomy_scores(scores, taxonomy_ids) -> dict[str, float]:  # type: ignore
    """Extract and sort taxonomy scores.

    Args:
        scores: A string or list of scores, which may contain numeric values or be comma-separated.
        taxonomy_ids: A string or list of taxonomy IDs corresponding to the scores.

    Returns:
        A dictionary mapping taxonomy IDs to their respective scores, sorted in descending order.
        Returns an empty dictionary if either input is None.
    """
    if scores is None or taxonomy_ids is None:
        return {}

    if isinstance(scores, str):
        scores = [float(score.strip()) for score in scores.split(",")]
    elif not isinstance(scores, list):
        scores = [float(score) for score in scores if not pd.isna(score)]

    if isinstance(taxonomy_ids, str):
        taxonomy_ids = [tax_id.strip() for tax_id in taxonomy_ids.split(",")]
    elif not isinstance(taxonomy_ids, list):
        taxonomy_ids = [
            tax_id
            for i, tax_id in enumerate(taxonomy_ids)
            if i < len(scores) and not pd.isna(scores[i])
        ]

    sorted_pairs = sorted(zip(scores, taxonomy_ids), reverse=True)
    return {str(tax_id): float(score) for score, tax_id in sorted_pairs}
