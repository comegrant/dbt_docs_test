from catboost import CatBoostClassifier
from pydantic import BaseModel
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier


class DataConfig(BaseModel):
    feature_table: str = "mlfeatures.ft_ml_recipes"
    feature_names: list[str] = [
        "cooking_time_from",
        "cooking_time_to",
        "cooking_time_mean",
        "recipe_difficulty_level_id",
        "recipe_main_ingredient_id",
        "number_of_taxonomies",
        "number_of_ingredients",
        "number_of_recipe_steps",
    ]
    excluded_columns: list[str] = [
        "recipe_id",
    ]
    primary_keys: list[str] = [
        "recipe_id",
    ]
    columns_to_encode: list[str] = ["recipe_difficulty_level_id", "recipe_main_ingredient_id"]
    columns_to_scale: list[str] = [
        "cooking_time_from",
        "cooking_time_to",
        "cooking_time_mean",
    ]

    target_mapped: dict[str, str] = {
        "has_chefs_favorite_taxonomy": "chefs_favorite",
        "has_family_friendly_taxonomy": "family_friendly",
    }


class ModelConfig:
    def __init__(self):
        self.classifier_mapping = {
            "AMK": {
                "has_chefs_favorite_taxonomy": self.get_xgboost_classifier(
                    n_estimator=141,
                    max_depth=8,
                    learning_rate=0.433,
                    subsample=0.774,
                    colsample_bytree=0.777,
                    gamma=0.460,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_xgboost_classifier(
                    n_estimator=158,
                    max_depth=5,
                    learning_rate=0.099,
                    subsample=0.741,
                    colsample_bytree=0.692,
                    gamma=0.696,
                    random_state=42,
                ),
            },
            "GL": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=156,
                    depth=6,
                    learning_rate=0.034,
                    l2_leaf_reg=0.0001,
                    random_strength=0.518,
                    bagging_temperature=0.084,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=50,
                    depth=7,
                    learning_rate=0.010,
                    l2_leaf_reg=0.038,
                    random_strength=0.620,
                    bagging_temperature=0.223,
                    verbose=False,
                    random_state=42,
                ),
            },
            "RT": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=194,
                    depth=9,
                    learning_rate=0.134,
                    l2_leaf_reg=0.0001,
                    random_strength=0.0001,
                    bagging_temperature=0.515,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=137,
                    depth=10,
                    learning_rate=0.203,
                    l2_leaf_reg=7.779,
                    random_strength=0.0001,
                    bagging_temperature=0.404,
                    verbose=False,
                    random_state=42,
                ),
            },
            "LMK": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=266,
                    depth=7,
                    learning_rate=0.233,
                    l2_leaf_reg=0.001,
                    random_strength=0.0001,
                    bagging_temperature=0.599,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=135,
                    depth=10,
                    learning_rate=0.283,
                    l2_leaf_reg=3.964,
                    random_strength=0.082,
                    bagging_temperature=0.404,
                    verbose=False,
                    random_state=42,
                ),
            },
        }

    @staticmethod
    def get_random_forest_classifier(**params):
        return RandomForestClassifier(**params)

    @staticmethod
    def get_catboost_classifier(**params):
        return CatBoostClassifier(**params)

    @staticmethod
    def get_xgboost_classifier(**params):
        return XGBClassifier(**params)

    def classifier(self, company, target):
        if company not in self.classifier_mapping or target not in self.classifier_mapping[company]:
            raise ValueError(f"No classifier found for company '{company}' and target '{target}'")
        return self.classifier_mapping[company][target]

    evaluation_metric = "f1"
