from catboost import CatBoostClassifier
from pydantic import BaseModel
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier


class FeatureLookupConfig(BaseModel):
    feature_table: str
    feature_names: list[str]
    primary_keys: list[str]


class DataConfig(BaseModel):
    recipe_feature_lookup: FeatureLookupConfig = FeatureLookupConfig(
        feature_table="mlfeatures.ft_ml_recipes",
        feature_names=[
            "cooking_time_from",
            "cooking_time_to",
            "cooking_time_mean",
            "recipe_difficulty_level_id",
            "recipe_main_ingredient_id",
            "number_of_taxonomies",
            "number_of_ingredients",
            "number_of_recipe_steps",
        ],
        primary_keys=[
            "recipe_id",
        ],
    )

    ingredient_feature_lookup: FeatureLookupConfig = FeatureLookupConfig(
        feature_table="mlfeatures.ft_recipe_ingredients",
        feature_names=[
            "has_chicken_filet",
            "has_chicken",
            "has_dry_pasta",
            "has_fresh_pasta",
            "has_white_fish_filet",
            "has_cod_fillet",
            "has_breaded_cod",
            "has_salmon_filet",
            "has_seafood",
            "has_pork_filet",
            "has_pork_cutlet",
            "has_trout_filet",
            "has_parmasan",
            "has_cheese",
            "has_minced_meat",
            "has_burger_patty",
            "has_noodles",
            "has_sausages",
            "has_tortilla",
            "has_pizza_crust",
            "has_bacon",
            "has_wok_sauce",
            "has_asian_sauces",
            "has_salsa",
            "has_flat_bread",
            "has_pita",
            "has_whole_salad",
            "has_shredded_vegetables",
            "has_potato",
            "has_peas",
            "has_rice",
            "has_nuts",
            "has_beans",
            "has_onion",
            "has_citrus",
            "has_sesame",
            "has_herbs",
            "has_fruit",
            "has_cucumber",
            "has_chili",
            "has_pancake",
        ],
        primary_keys=["recipe_portion_id", "language_id"],
    )

    excluded_columns: list[str] = [
        "recipe_id",
        "recipe_portion_id",
        "language_id",
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
                    n_estimator=176,
                    max_depth=9,
                    learning_rate=0.253,
                    subsample=0.910,
                    colsample_bytree=0.894,
                    gamma=0.139,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=247,
                    depth=9,
                    learning_rate=0.110,
                    l2_leaf_reg=0.0001,
                    random_strength=0.0001,
                    bagging_temperature=0.0,
                    verbose=False,
                    random_state=42,
                ),
            },
            "GL": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=133,
                    depth=10,
                    learning_rate=0.083,
                    l2_leaf_reg=0.082,
                    random_strength=0.0001,
                    bagging_temperature=0.782,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=262,
                    depth=8,
                    learning_rate=0.103,
                    l2_leaf_reg=0.224,
                    random_strength=0.001,
                    bagging_temperature=0.909,
                    verbose=False,
                    random_state=42,
                ),
            },
            "RT": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=133,
                    depth=9,
                    learning_rate=0.249,
                    l2_leaf_reg=0.201,
                    random_strength=0.0001,
                    bagging_temperature=0.537,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=275,
                    depth=10,
                    learning_rate=0.162,
                    l2_leaf_reg=0.107,
                    random_strength=5.105,
                    bagging_temperature=0.645,
                    verbose=False,
                    random_state=42,
                ),
            },
            "LMK": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=227,
                    depth=7,
                    learning_rate=0.237,
                    l2_leaf_reg=0.0001,
                    random_strength=0.222,
                    bagging_temperature=0.231,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=300,
                    depth=10,
                    learning_rate=0.178,
                    l2_leaf_reg=0.0001,
                    random_strength=0.0001,
                    bagging_temperature=0.753,
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
