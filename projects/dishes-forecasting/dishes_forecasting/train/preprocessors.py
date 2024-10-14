import nltk
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OneHotEncoder


class TaxonomyOneHotTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, top_n: int = 50):
        self.encoder = OneHotEncoder(handle_unknown='ignore')
        self.top_n = top_n

    def fit(self, X, y=None): # noqa
        all_taxonomies = X["taxonomy_list"].str.split(", ").explode().to_list()
        # Find the to taxonomies
        top_taxonomies = list(
            pd.Series(all_taxonomies).value_counts().head(self.top_n).index
        )
        self.encoder.fit(np.array(list(top_taxonomies)).reshape(-1, 1))
        return self

    def transform(self, X): # noqa
        # Create a matrix of zeros
        result = np.zeros((len(X), len(self.encoder.categories_[0])))

        for i, taxonomies in enumerate(X["taxonomy_list"]):
            taxonomies_str = str(taxonomies)
            if taxonomies:
                # Transform only if the list is not empty
                taxonomy_list = taxonomies_str.split(", ")
                encoded = self.encoder.transform(np.array(taxonomy_list).reshape(-1, 1))
                # Sum up the encodings for each taxonomy in the list
                result[i] = encoded.sum(axis=0)
        return result

    def get_feature_names_out(self, input_features=None): # noqa
        return self.encoder.get_feature_names_out()


def get_stopwords(
    company_code: str
) -> list[str]:
    nltk.download("stopwords")
    if company_code in ("GL", "AMK"):
        stopwords_country = list(set(stopwords.words('norwegian')))
    elif company_code == "LMK":
        stopwords_country = list(set(stopwords.words('swedish')))
    elif company_code == "RT":
        stopwords_country = list(set(stopwords.words('danish')))
    return stopwords_country
