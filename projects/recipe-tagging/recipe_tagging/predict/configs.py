from typing import Callable, Any
from pydantic import BaseModel
from recipe_tagging.predict.taxonomy_extractors import (
    extract_preferences,
    extract_proteins,
    extract_categories,
    extract_dishes,
    extract_traits,
)

from recipe_tagging.predict.mappings import (
    protein_category_mapping,
    dish_mapping,
    preference_mapping,
    protein_mapping,
    trait_mapping,
    cuisine_mapping,
)


class SEOConfig(BaseModel):
    mapping: dict[str, Any]
    extract_func: Callable


class PredictConfig(BaseModel):
    seo_types: list[str] = [
        "cuisine",
        "preferences",
        "protein",
        "protein_category",
        "dish_type",
        "trait",
    ]

    seo_config: dict[str, SEOConfig] = {
        "preferences": SEOConfig(
            mapping=preference_mapping,
            extract_func=extract_preferences,
        ),
        "protein": SEOConfig(
            mapping=protein_mapping,
            extract_func=extract_proteins,
        ),
        "protein_category": SEOConfig(
            mapping=protein_category_mapping,
            extract_func=extract_categories,
        ),
        "dish_type": SEOConfig(
            mapping=dish_mapping,
            extract_func=extract_dishes,
        ),
        "trait": SEOConfig(
            mapping=trait_mapping,
            extract_func=extract_traits,
        ),
        "cuisine": SEOConfig(
            mapping=cuisine_mapping,
            extract_func=lambda x: x,  # no function for this
        ),
    }

    output_columns: list[str] = [
        "language",
        "recipe_id",
        "taxonomy_preferences_name",
        "taxonomy_preferences_score",
        "taxonomy_preferences_id",
        "taxonomy_protein_category_name",
        "taxonomy_protein_category_score",
        "taxonomy_protein_category_id",
        "taxonomy_protein_name",
        "taxonomy_protein_score",
        "taxonomy_protein_id",
        "taxonomy_dish_type_name",
        "taxonomy_dish_type_score",
        "taxonomy_dish_type_id",
        "taxonomy_trait_name",
        "taxonomy_trait_score",
        "taxonomy_trait_id",
        "taxonomy_cuisine_name",
        "taxonomy_cuisine_score",
        "taxonomy_cuisine_id",
    ]

    output_table_name: str = "mlgold.recipe_tagging"
