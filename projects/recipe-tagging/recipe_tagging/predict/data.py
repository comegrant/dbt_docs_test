import pandera as pa
import pandas as pd
from pyspark.sql import SparkSession
from recipe_tagging.db import get_data_from_sql
from recipe_tagging.paths import PREDICT_SQL_DIR

recipe_schema = pa.DataFrameSchema(
    {
        "generic_ingredient_name_list": pa.Column(object, nullable=True),
        "recipe_main_ingredient_name_local": pa.Column(str, nullable=True),
        "taxonomy_name_list": pa.Column(object, nullable=True),
        "preference_name_combinations": pa.Column(object, nullable=True),
        "recipe_name": pa.Column(str, nullable=True),
    },
    strict=False,
    coerce=True,
)


seo_taxonomy_schema = pa.DataFrameSchema(
    {
        "taxonomy_type_name": pa.Column(str),
        "taxonomy_id": pa.Column(int),
        "language_id": pa.Column(int),
        "taxonomy_name_local": pa.Column(str),
        "taxonomy_name_english": pa.Column(str),
    },
    strict=False,
    coerce=True,
)


def get_recipe_data(spark: SparkSession, language_id: int) -> pd.DataFrame:
    """Get the recipe data with schema validation."""

    df = get_data_from_sql(
        spark=spark,
        sql_path=PREDICT_SQL_DIR / "data_query.sql",
        language_id=language_id,
    ).toPandas()

    try:
        validated_df = recipe_schema.validate(df)
        return validated_df
    except Exception as e:
        raise ValueError(f"Recipe data validation failed: {e}")


def get_seo_taxonomy(spark: SparkSession, language_id: int) -> pd.DataFrame:
    """Get the seo taxonomies for recipe tagging with schema validation."""

    df = get_data_from_sql(
        spark=spark,
        sql_path=PREDICT_SQL_DIR / "seo_taxonomy_query.sql",
        language_id=language_id,
    ).toPandas()

    try:
        validated_df = seo_taxonomy_schema.validate(df)
        return validated_df
    except Exception as e:
        raise ValueError(f"SEO taxonomy data validation failed: {e}")
