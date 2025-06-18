import json
from datetime import datetime
from typing import Callable

import logging
import pandas as pd
from data_contracts.sources import data_science_data_lake
from pyspark.sql import SparkSession
from recipe_tagging.predict.configs import PredictConfig
from recipe_tagging.predict.data import get_recipe_data, get_seo_taxonomy

from recipe_tagging.predict.processing import (
    preprocess_data,
    seo_tag_to_taxonomy_id,
    extract_sorted_taxonomy_scores,
)
from recipe_tagging.predict.taxonomy_extractors import get_cuisines
from recipe_tagging.common import Args

logger = logging.getLogger(__name__)
config = PredictConfig()


def process_taxonomy(
    df: pd.DataFrame,
    seo_data: pd.DataFrame,
    mapping: dict,
    extractor_func: Callable,
    taxonomy_type: str,
    **kwargs,
) -> pd.DataFrame:
    """Process taxonomy extraction.

    Args:
        df: DataFrame containing recipe data.
        seo_data: DataFrame containing SEO taxonomy data.
        mapping: Dictionary mapping taxonomy terms.
        extractor_func: Callable function for extracting taxonomies.
        taxonomy_type: Type of SEO taxonomy to process, e.g. "protein".
        **kwargs: Additional arguments passed to extractor func.

    Returns:
        DataFrame with added taxonomy columns for names, IDs, and scores
    """
    taxonomy_to_id = seo_tag_to_taxonomy_id(seo_data)

    def apply_extractor(row: pd.Series) -> pd.Series:
        matched_items, matched_scores = extractor_func(
            row, mapping, taxonomy_to_id, **kwargs
        )

        matched_ids = {
            str(taxonomy_to_id[item])
            for item in matched_items
            if item in taxonomy_to_id
        }

        return pd.Series(
            [
                ", ".join(matched_items) if matched_items else None,
                ", ".join(matched_ids) if matched_ids else None,
                ", ".join(str(matched_scores[item]) for item in matched_items)
                if matched_items
                else None,
            ]
        )

    column_names = [
        f"taxonomy_{taxonomy_type}_name",
        f"taxonomy_{taxonomy_type}_id",
        f"taxonomy_{taxonomy_type}_score",
    ]

    df[column_names] = df.apply(apply_extractor, axis=1)
    return df


def predict_pipeline(spark: SparkSession, args: Args) -> pd.DataFrame:
    """Main prediction pipeline for recipe tagging.

    Args:
        spark: Spark session
        args: Arguments

    Returns:
        DataFrame with recipe data and taxonomy tags
    """

    logger.info("Getting recipe data.")
    recipe_data = get_recipe_data(spark=spark, language_id=args.language_id)
    recipe_data = preprocess_data(recipe_data)

    logger.info("Getting taxonomy data.")
    taxonomy_data = get_seo_taxonomy(spark=spark, language_id=args.language_id)

    taxonomy_by_type = {
        seo_type: pd.DataFrame(
            taxonomy_data[taxonomy_data["taxonomy_type_name"] == f"seo_{seo_type}"]
        )
        for seo_type in config.seo_types
    }

    for seo_type in config.seo_types:
        logger.info(f"Processing {seo_type}.")
        taxonomy_mapping = config.seo_config[seo_type].mapping
        extract_func = config.seo_config[seo_type].extract_func
        if seo_type == "cuisine":
            recipe_data = get_cuisines(
                recipe_data,
                taxonomy_by_type[seo_type],
                taxonomy_mapping,
                args,
            )
        else:
            recipe_data = process_taxonomy(
                recipe_data,
                taxonomy_by_type[seo_type],
                taxonomy_mapping,
                extract_func,
                seo_type,
            )

    recipe_data["language"] = args.language_short
    return recipe_data


async def send_to_azure(df: pd.DataFrame, args: Args) -> None:
    """Send the results to Azure storage asynchronously.

    Args:
        df: DataFrame containing recipe data with taxonomy tags
        args: Arguments
    """
    logger.info("Sending results to Azure storage.")

    success = True
    processed_recipes = 0
    total_recipes = len(df)
    json_output = {"success": False, "recipes": []}
    data = pd.DataFrame()

    logger.info("Processing recipes.")
    try:
        data = df[config.output_columns].reset_index(drop=True)
        for _, row in data.iterrows():
            taxonomy_ids = {}

            for seo_type in config.seo_types:
                taxonomy_ids.update(
                    extract_sorted_taxonomy_scores(
                        row[f"taxonomy_{seo_type}_score"],
                        row[f"taxonomy_{seo_type}_id"],
                    )
                )

            json_output["recipes"].append(
                {
                    "recipe_id": row["recipe_id"],
                    f"taxonomy_ids_{row['language']}": taxonomy_ids,
                }
            )
            processed_recipes += 1
    except Exception as e:
        success = False
        logger.error(f"Error processing recipes: {e}")

    if processed_recipes != total_recipes:
        success = False
        logger.error(f"Processed {processed_recipes} recipes out of {total_recipes}")

    json_output["success"] = success

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("Writing results to Azure storage.")
    await data_science_data_lake.config.storage.write(
        path=f"data-science/recipe_tagging/{args.env}/{timestamp}_output.json",
        content=json.dumps(json_output).encode("utf-8"),
    )
