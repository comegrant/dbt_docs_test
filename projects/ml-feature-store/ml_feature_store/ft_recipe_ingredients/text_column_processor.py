from collections.abc import Iterator
from typing import Optional

import pandas as pd
import spacy
from spacy.tokens.doc import Doc

from ml_feature_store.ft_recipe_ingredients.configs.languages import language_mapping


def add_nouns_only_and_lemmatized_columns(
    df_distinct_ingredients: pd.DataFrame,
    text_column_name: str,
    nouns_only_column_name: Optional[str] = "nouns_only",
    lemmatized_column_name: Optional[str] = None,
    is_fillna: Optional[bool] = True
) -> pd.DataFrame:

    df_with_nouns_only = add_nouns_only_column(
        df_distinct_ingredients=df_distinct_ingredients,
        text_col_name=text_column_name,
        nouns_only_col_name=nouns_only_column_name,
        is_fillna=is_fillna
    )

    df_lemmatized_nouns = add_lemmatized_column(
        df_distinct_ingredients=df_with_nouns_only,
        text_col_name=nouns_only_column_name,
        lemmatized_col_name=lemmatized_column_name,
        is_fillna=is_fillna
    )
    return df_lemmatized_nouns


def add_nouns_only_column(
    df_distinct_ingredients: pd.DataFrame,
    text_col_name: str,
    nouns_only_col_name: Optional[str] = None,
    is_fillna: Optional[bool] = True
) -> pd.DataFrame:

    def keep_nouns(docs: Iterator[Doc]) -> list[str]:
        return [' '.join([token.text.lower() for token in doc if token.pos_ == 'NOUN']) for doc in docs]
    if nouns_only_col_name is None:
        nouns_only_col_name = text_col_name + "_nouns_only"

    # First set default to be the same as the column to be processed
    df_distinct_ingredients[nouns_only_col_name] = df_distinct_ingredients[text_col_name]
    language_ids = df_distinct_ingredients["language_id"].unique()

    for language_id in language_ids:
        # Load the corresponded spacy langauge model
        spacy_model = language_mapping[language_id]["spacy_model"]
        nlp = spacy.load(spacy_model)
        # Find the indices that contains the language
        row_idx = df_distinct_ingredients[
            df_distinct_ingredients["language_id"] == language_id
        ].index

        df_one_language = df_distinct_ingredients.loc[row_idx, text_col_name]
        docs = list(nlp.pipe(df_one_language))
        results = keep_nouns(docs=docs)
        df_distinct_ingredients.loc[row_idx, nouns_only_col_name] = results
    if is_fillna:
        row_indexes_with_null = df_distinct_ingredients[
            df_distinct_ingredients[nouns_only_col_name] == ""
        ].index

        df_distinct_ingredients.loc[row_indexes_with_null, nouns_only_col_name] = (
            df_distinct_ingredients.loc[row_indexes_with_null, text_col_name]
        )
    return df_distinct_ingredients


def add_lemmatized_column(
    df_distinct_ingredients: pd.DataFrame,
    text_col_name: str,
    lemmatized_col_name: Optional[str] = None,
    is_fillna: Optional[bool] = True
) -> pd.DataFrame:

    def keep_nouns(docs: Iterator[Doc]) -> list[str]:
        return [' '.join([token.lemma_.lower() for token in doc]) for doc in docs]
    if lemmatized_col_name is None:
        lemmatized_col_name = text_col_name + "_lemmatized"

    # First set default to be the same as the column to be processed
    df_distinct_ingredients[lemmatized_col_name] = df_distinct_ingredients[text_col_name]
    language_ids = df_distinct_ingredients["language_id"].unique()

    for language_id in language_ids:
        # Load the corresponded spacy langauge model
        spacy_model = language_mapping[language_id]["spacy_model"]
        nlp = spacy.load(spacy_model)
        # Find the indices that contains the language
        row_idx = df_distinct_ingredients[
            df_distinct_ingredients["language_id"] == language_id
        ].index

        df_one_language = df_distinct_ingredients.loc[row_idx, text_col_name]
        docs = list(nlp.pipe(df_one_language))
        results = keep_nouns(docs=docs)
        df_distinct_ingredients.loc[row_idx, lemmatized_col_name] = results
    if is_fillna:
        row_indexes_with_null = df_distinct_ingredients[
            df_distinct_ingredients[lemmatized_col_name] == ""
        ].index

        df_distinct_ingredients.loc[row_indexes_with_null, lemmatized_col_name] = (
            df_distinct_ingredients.loc[row_indexes_with_null, text_col_name]
        )
    return df_distinct_ingredients
