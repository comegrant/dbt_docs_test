from typing import Set, Tuple

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from recipe_tagging.predict.processing import (
    seo_tag_to_taxonomy_id,
    simple_match,
    reverse_keyword_mapping,
)
from recipe_tagging.common import Args
from recipe_tagging.predict.mappings import (
    dish_penalty_words,
    protein_penalty_pairs,
)

# CONSTANTS
PROTEIN_THRESH: int = 87
PROTEIN_PENALTY: int = 20
CUISINE_THRESH: float = 0.20
MAX_COOKING_TIME: int = 30


def extract_preferences(
    row: pd.Series, mapping: dict, taxonomy_to_id: dict, **kwargs
) -> Tuple[Set[str], dict[str, float]]:
    """Extract preferences based on main ingredient, negative preferences, and nutritional facts."""
    matched_items = set()
    keywords = reverse_keyword_mapping(mapping)

    for column, preference in keywords["column"].items():
        if row[column] is True:
            matched_items.add(preference)

    matched_items = simple_match(
        row["recipe_main_ingredient_name_local"],
        keywords["main_ingredient"],
        matched_items,
    )

    allergens_set = row["preference_name_combinations"]
    for keyword, preference in keywords["negative_preference"].items():
        if keyword in allergens_set:
            continue
        matched_items.add(preference)

    matched_scores = dict.fromkeys(matched_items, 1.0)

    return matched_items, matched_scores


def extract_categories(
    row: pd.Series, mapping: dict, taxonomy_to_id: dict, **kwargs
) -> Tuple[Set[str], dict[str, float]]:
    """Extract categories based on main ingredient and ingredients."""
    matched_items = set()
    keywords = reverse_keyword_mapping(mapping)

    matched_items = simple_match(
        row["recipe_main_ingredient_name_local"],
        keywords["main_ingredient"],
        matched_items,
    )
    matched_items = simple_match(
        row["ingredient_list"],
        keywords["ingredient"],
        matched_items,
    )

    matched_scores = dict.fromkeys(matched_items, 1.0)

    return matched_items, matched_scores


def extract_proteins(
    row: pd.Series, mapping: dict, taxonomy_to_id: dict, **kwargs
) -> Tuple[Set[str], dict[str, float]]:
    """Extract proteins based on fuzzy matching of ingredients."""
    matched_items = set()
    keywords = reverse_keyword_mapping(mapping)

    ingredients = row["ingredient_list"]

    for ingredient in ingredients:
        if ingredient in keywords["ingredient"]:
            matched_items.add(keywords["ingredient"][ingredient])

        else:
            for keyword in keywords["ingredient"]:
                sim_score = fuzz.partial_ratio(ingredient, keyword)

                if (ingredient, keyword) in protein_penalty_pairs or (
                    keyword,
                    ingredient,
                ) in protein_penalty_pairs:
                    sim_score -= PROTEIN_PENALTY

                if sim_score > PROTEIN_THRESH:
                    matched_items.add(keywords["ingredient"][keyword])

    matched_scores = dict.fromkeys(matched_items, 1.0)

    return matched_items, matched_scores


def extract_traits(
    row: pd.Series, mapping: dict, taxonomy_to_id: dict, **kwargs
) -> Tuple[Set[str], dict[str, float]]:
    """Extract trait taxonomy based on cooking time, taxonomies, and ingredients/recipe names."""
    matched_items = set()
    keywords = reverse_keyword_mapping(mapping)

    for column, preference in keywords["column"].items():
        if row[column] <= MAX_COOKING_TIME:
            matched_items.add(preference)

    matched_items = simple_match(
        row["taxonomy_name_list"],
        keywords["taxonomy"],
        matched_items,
    )
    matched_items = simple_match(
        row["ingredient_list"],
        keywords["ingredient"],
        matched_items,
    )
    matched_items = simple_match(
        row["recipe_name"],
        keywords["ingredient"],
        matched_items,
    )

    matched_scores = dict.fromkeys(matched_items, 1.0)

    return matched_items, matched_scores


def extract_dishes(
    row: pd.Series, mapping: dict, taxonomy_to_id: dict, **kwargs
) -> Tuple[Set[str], dict[str, float]]:
    """Extract dish types based on recipe name."""
    matched_items = set()
    keywords = reverse_keyword_mapping(mapping)

    recipe_names = row["recipe_name"]

    for recipe_name in recipe_names:
        for keyword in keywords["recipe_name"]:
            if keyword in recipe_name:
                should_add = True
                for word in dish_penalty_words:
                    if word in recipe_name:
                        should_add = False
                        break

                if should_add:
                    matched_items.add(keywords["recipe_name"][keyword])

    matched_scores = dict.fromkeys(matched_items, 1.0)

    return matched_items, matched_scores


def get_cuisines(
    df: pd.DataFrame, seo_category: pd.DataFrame, mapping: dict, args: Args
) -> pd.DataFrame:
    """Process top cuisine taxonomy using model predictions.

    Args:
        df: DataFrame containing recipe data
        seo_category: DataFrame with SEO data
        mapping: Dictionary with cuisine mapping
        args: Arguments

    Returns:
        DataFrame with cuisine taxonomy name, taxonomy ids, and prediction probability added
    """
    cuisine_to_taxonomy_id = seo_tag_to_taxonomy_id(seo_category)
    model_uri = f"models:/{args.env}.mloutputs.recipe_tagging_{args.language}@champion"

    try:
        mlflow.set_registry_uri("databricks-uc")
        pipeline = mlflow.sklearn.load_model(model_uri)

        if pipeline is None:
            raise RuntimeError("Model loaded as None")
    except Exception as e:
        raise RuntimeError(f"Error loading model from {model_uri}: {e}")

    y_pred_proba = pipeline.predict_proba(df["generic_ingredient_name_list"])
    class_labels = pipeline.named_steps["classifier"].classes_

    def get_top_predictions(
        proba_matrix: np.ndarray, class_labels: np.ndarray, threshold: float
    ) -> tuple[list, list]:
        """Get the top predictions (up to 3) OR those with probability > threshold"""
        results_classes = []
        results_probas = []

        for proba in proba_matrix:
            sorted_indices = np.argsort(proba)[::-1]
            sorted_probas = proba[sorted_indices]
            sorted_classes = [class_labels[i] for i in sorted_indices]

            valid_mask = sorted_probas >= threshold
            filtered_classes = np.array(sorted_classes)[valid_mask]
            filtered_probas = sorted_probas[valid_mask]

            results_classes.append(list(filtered_classes[:3]))
            results_probas.append(list(filtered_probas[:3]))

        return results_classes, results_probas

    top_pred_classes, top_pred_probas = get_top_predictions(
        y_pred_proba, class_labels, threshold=CUISINE_THRESH
    )

    preds = pd.DataFrame(
        {
            "taxonomy_cuisine_name": top_pred_classes,
            "taxonomy_cuisine_score": top_pred_probas,
        }
    )

    final = pd.concat([df, preds], axis=1)
    final["taxonomy_cuisine_id"] = final["taxonomy_cuisine_name"].apply(
        lambda x: [cuisine_to_taxonomy_id[cuisine] for cuisine in x]
    )

    return final
