import re

import nltk
import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer

from dishes_forecasting.logger_config import logger


def build_features(
    df_weekly_variations: pd.DataFrame,
    df_recipes: pd.DataFrame,
    df_recipe_ingredients: pd.DataFrame,
    df_recipe_price_ratings: pd.DataFrame,
    feature_configs: dict,
    language: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_weekly_dishes_features = build_weekly_dishes_features(
        df_weekly_variations=df_weekly_variations,
        df_recipe_price_ratings=df_recipe_price_ratings,
    )

    df_recipe_features = build_recipe_features(
        df_recipes=df_recipes,
        df_recipe_ingredients=df_recipe_ingredients,
        language=language,
        ingredient_encode_method=feature_configs["ingredient_encode_method"],
    )

    return df_weekly_dishes_features, df_recipe_features


def build_weekly_dishes_features(
    df_weekly_variations: pd.DataFrame,
    df_recipe_price_ratings: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Creating weekly dishes features...")
    df_weekly_dishes = df_weekly_variations.merge(
        df_recipe_price_ratings,
        on=["year", "week", "company_id", "recipe_portion_id"],
        how="left",
    )

    return df_weekly_dishes


def build_recipe_features(
    df_recipes: pd.DataFrame,
    df_recipe_ingredients: pd.DataFrame,
    language: str,
    ingredient_encode_method: str | None = "tfidf",
) -> pd.DataFrame:
    logger.info(
        f"Creating recipe ingredient feature using {ingredient_encode_method}...",
    )
    df_recipe_features = add_recipe_ingredient_feature(
        df_recipes=df_recipes,
        df_recipe_ingredients=df_recipe_ingredients,
        language=language,
        method=ingredient_encode_method,
    )
    logger.info("Creating tags one-hot features...")
    df_recipe_features = add_tags_dummies(
        df=df_recipe_features,
        tags_columns=[
            "internal_tags",
            "category_tags",
            "special_food_tags",
            "recommended_tags",
        ],
    )

    logger.info("Creating portions one-hot features...")
    df_recipe_features = add_one_hot_feature(
        df=df_recipe_features,
        one_hot_col="portions",
    )
    logger.info("Creating main ingredient one-hot features...")
    df_recipe_features = add_one_hot_feature(
        df=df_recipe_features,
        one_hot_col="recipe_main_ingredient_name",
    )
    df_recipe_features = df_recipe_features.drop_duplicates(subset="recipe_portion_id")
    return df_recipe_features


def add_recipe_ingredient_feature(
    df_recipes: pd.DataFrame,
    df_recipe_ingredients: pd.DataFrame,
    language: str,
    method: str | None = "tfidf",
) -> pd.DataFrame:
    df_ingredients_agg = (
        df_recipe_ingredients.groupby("recipe_portion_id")
        .agg(ingredient_agg=("ingredient_name", " ".join))
        .reset_index()
    )
    df_recipes_merged = df_recipes.merge(
        df_ingredients_agg,
        on="recipe_portion_id",
        how="left",
    )
    df_recipes_merged["recipe_name"] = df_recipes_merged["recipe_name"].fillna("")
    df_recipes_merged["ingredient_agg"] = df_recipes_merged["ingredient_agg"].fillna("")
    df_recipes_merged["ingredient_recipe_name"] = (
        df_recipes_merged["recipe_name"] + " " + df_recipes_merged["ingredient_agg"]
    )
    df_recipes_merged["ingredient_recipe_name"] = (
        df_recipes_merged["ingredient_recipe_name"]
        .str.lower()
        .apply(lambda x: re.sub("[^a-zäåØøæ]", " ", x))
    )

    stop_words = get_stopwords(language=language)
    df_tfidf = get_tfidf_features(
        df=df_recipes_merged,
        col="ingredient_recipe_name",
        stop_words=stop_words,
    )
    if method == "one_hot":
        # Replace all none zeros with 1
        threshold = 0.0000001
        df_tfidf = df_tfidf.where(df_tfidf < threshold, 1)

    df = pd.concat(
        [
            df_recipes_merged.drop(
                columns=["recipe_name", "ingredient_agg", "ingredient_recipe_name"],
            ),
            df_tfidf,
        ],
        axis=1,
    )
    return df


def add_tags_dummies(df: pd.DataFrame, tags_columns: list[str]) -> pd.DataFrame:
    df = df.assign(tags="")
    for colname in tags_columns:
        df["tags"] = df["tags"].str.cat(
            df[colname],
            sep=", ",
            na_rep="missing",
            join="outer",
        )
    df["tags"] = df["tags"].apply(lambda x: x[1:])
    df["tags"] = df["tags"].str.replace(" ", "")
    df_tags_dummmies = df["tags"].str.get_dummies(",").drop(columns="missing")
    df_tags_dummmies.columns = "tag_" + df_tags_dummmies.columns
    df = df.drop(columns=tags_columns)
    df = df.drop(columns="tags")
    df = pd.concat([df, df_tags_dummmies], axis=1)
    return df


def add_one_hot_feature(df: pd.DataFrame, one_hot_col: str) -> pd.DataFrame:
    df[one_hot_col] = df[one_hot_col].astype(str)
    df_dummies = df[one_hot_col].str.get_dummies()

    # Append dummy column names with a prefix that is portions_
    prefix = one_hot_col + "_"
    df_dummies.columns = prefix + df_dummies.columns.str.lower()

    # Join with original dataframe, drop the original non-one-hot column
    df = df.join(df_dummies).drop(columns=one_hot_col, axis=1)

    return df


def get_tfidf_features(
    df: pd.DataFrame,
    col: str,
    stop_words: list | None = None,
) -> pd.DataFrame:
    vectorizer = TfidfVectorizer(
        stop_words=stop_words,
        use_idf=True,
    )
    tfidf_mat = vectorizer.fit_transform(df[col])
    df_tfidf = pd.DataFrame(tfidf_mat.toarray())
    df_tfidf.columns = vectorizer.get_feature_names_out()
    return df_tfidf


def get_stopwords(language: str) -> list[str]:
    nltk.download("stopwords")
    stop_words = list(set(stopwords.words(language.lower())))
    return stop_words
