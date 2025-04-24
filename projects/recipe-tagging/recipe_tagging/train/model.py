import re
from typing import Optional, Union

import pandas as pd
from bayes_opt import BayesianOptimization
from mlflow.models import ModelSignature, infer_signature
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from recipe_tagging.train.configs import ModelConfig
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC

config = ModelConfig()


class IngredientPreprocessor(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.lemmatizer = WordNetLemmatizer()

    def fit(
        self, X: pd.Series, y: Optional[pd.Series] = None
    ) -> "IngredientPreprocessor":  # noqa
        return self

    def transform(self, X: pd.Series) -> pd.Series:  # noqa
        return pd.Series(X.apply(self._process))

    def _process(self, text: Union[str, list]) -> str:
        if isinstance(text, list):
            tokens = text
        else:
            tokens = str(text).split(", ")

        tokens = [
            " ".join([self.lemmatizer.lemmatize(q) for q in p.split()]) for p in tokens
        ]
        tokens = [re.sub(r"[^a-zA-ZæøåäöÆØÅÄÖ]", " ", t) for t in tokens]
        return " ".join(tokens).lower()


def train_model(
    df: pd.DataFrame, language: str
) -> tuple[Pipeline, ModelSignature, pd.DataFrame]:
    preprocessor = IngredientPreprocessor()

    features = preprocessor.fit_transform(df["generic_ingredient_name_list"])
    target = df["cuisine_name"]

    tfidf = TfidfVectorizer(
        stop_words=stopwords.words(language),
        ngram_range=config.tfidf.ngram_range,
        max_df=config.tfidf.max_df,
        analyzer=config.tfidf.analyzer,
        binary=config.tfidf.binary,
        token_pattern=config.tfidf.token_pattern,
        sublinear_tf=config.tfidf.sublinear_tf,
    )

    train = tfidf.fit_transform(features)

    def evalfn(C: float, gamma: float) -> float:  # noqa
        model = SVC(
            C=float(C),
            gamma=float(gamma),  # pyright: ignore
            kernel=config.svc.kernel,
            class_weight=config.svc.class_weight,
            probability=config.svc.probability,
        )
        scores = cross_val_score(
            model, train, target, cv=config.cv.n_splits, scoring=config.cv.scoring
        )
        return scores.max()

    optimizer = BayesianOptimization(
        evalfn, {"C": config.hyperparam.C, "gamma": config.hyperparam.gamma}
    )
    optimizer.maximize(
        init_points=config.bayes_opt.init_points, n_iter=config.bayes_opt.n_iter
    )
    if optimizer.max is None:
        raise ValueError("Best parameters are None")
    best_params = optimizer.max["params"]

    clf = SVC(
        C=float(best_params["C"]),
        gamma=float(best_params["gamma"]),  # pyright: ignore
        kernel=config.svc.kernel,
        class_weight=config.svc.class_weight,
        probability=config.svc.probability,
    )
    clf.fit(train, target)

    input_example = pd.DataFrame({"ingredients": df["generic_ingredient_name_list"]})
    model_pipeline = Pipeline(
        [
            ("preprocessor", IngredientPreprocessor()),
            ("vectorizer", tfidf),
            ("classifier", clf),
        ]
    )
    predictions = model_pipeline.predict(input_example["ingredients"])
    signature = infer_signature(input_example, predictions)

    return model_pipeline, signature, input_example
