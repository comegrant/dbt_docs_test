from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor, VotingRegressor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

from dishes_forecasting.train.preprocessors import TaxonomyOneHotTransformer, get_stopwords


def define_ensemble_model(
    company_code: str,
    params_lgb: dict,
    params_xgb: dict,
    params_rf: dict
) -> Pipeline:
    categorical_features = [
        "cooking_time_from",
        "cooking_time_to",
        "menu_year",
        "menu_week",
        "portion_size",
        "recipe_difficulty_level_id",
        "recipe_main_ingredient_id",
    ]
    taxonomy_features = ["taxonomy_list"]
    # Create preprocessing steps
    categorical_transformer = OneHotEncoder(handle_unknown="ignore")
    custom_transformer = TaxonomyOneHotTransformer(top_n=250)

    # Define the TF-IDF preprocessing for recipe_name
    stop_words = get_stopwords(company_code=company_code)
    tfidf_transformer = TfidfVectorizer(stop_words=stop_words, binary=True, max_features=200)

    # Combine preprocessing steps
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", categorical_transformer, categorical_features),
            ("taxonomies", custom_transformer, taxonomy_features),
            ("tfidf_recipe", tfidf_transformer, "recipe_name"),
        ],
        remainder="passthrough"
    )

    lgbm = LGBMRegressor(**params_lgb)
    rf = RandomForestRegressor(**params_rf)
    xgb = XGBRegressor(**params_xgb)
    ensemble = VotingRegressor(
        estimators=[
            ("lgbm", lgbm),
            ("rf", rf),
            ("xgb", xgb),
        ],
        n_jobs=-1
    )
    # Create final pipeline with preprocessor and ensemble
    custom_pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("ensemble", ensemble),
        ]
    )

    return custom_pipeline
