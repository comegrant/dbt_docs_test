from catboost import CatBoostClassifier
from pydantic import BaseModel
from sklearn.ensemble import RandomForestClassifier


class FeatureConfig(BaseModel):
    schema_name: str
    table_name: str
    feature_names: list[str]


class DataConfig(BaseModel):
    recipe_features: FeatureConfig = FeatureConfig(
        schema_name="mlgold",
        table_name="attribute_scoring_recipes",
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
    )

    ingredient_features: FeatureConfig = FeatureConfig(
        schema_name="mlgold",
        table_name="dishes_forecasting_recipe_ingredients",
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
    )

    columns_to_encode: list[str] = [
        "recipe_difficulty_level_id",
        "recipe_main_ingredient_id",
    ]
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
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=298,
                    depth=10,
                    learning_rate=0.16717457651299977,
                    l2_leaf_reg=0.28301679031533256,
                    random_strength=0.000001,
                    bagging_temperature=0.7469602122179738,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=119,
                    depth=10,
                    learning_rate=0.27559794001069593,
                    l2_leaf_reg=0.7117900290895602,
                    random_strength=0.000001,
                    bagging_temperature=0.1839950418425128,
                    verbose=False,
                    random_state=42,
                ),
            },
            "GL": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=235,
                    depth=10,
                    learning_rate=0.19382830457190459,
                    l2_leaf_reg=2.845940732112113e-05,
                    random_strength=2.449313015936421,
                    bagging_temperature=0.005730276705875004,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=148,
                    depth=9,
                    learning_rate=0.19315209748280762,
                    l2_leaf_reg=0.060245049868137036,
                    random_strength=0.000001,
                    bagging_temperature=0.4722624028596675,
                    verbose=False,
                    random_state=42,
                ),
            },
            "RT": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=209,
                    depth=9,
                    learning_rate=0.075764694896021,
                    l2_leaf_reg=2.3017835161449583e-05,
                    random_strength=0.0004342837489409636,
                    bagging_temperature=0.2912439354780485,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=270,
                    depth=7,
                    learning_rate=0.058474895735769715,
                    l2_leaf_reg=6.028316064181121e-08,
                    random_strength=0.005892003907765736,
                    bagging_temperature=0.3186740577458742,
                    verbose=False,
                    random_state=42,
                ),
            },
            "LMK": {
                "has_chefs_favorite_taxonomy": self.get_catboost_classifier(
                    iterations=232,
                    depth=10,
                    learning_rate=0.09829383174658538,
                    l2_leaf_reg=8.10414949651025,
                    random_strength=0.008946359282479863,
                    bagging_temperature=0.6761204271222518,
                    verbose=False,
                    random_state=42,
                ),
                "has_family_friendly_taxonomy": self.get_catboost_classifier(
                    iterations=150,
                    depth=9,
                    learning_rate=0.12443396566977571,
                    l2_leaf_reg=0.0004877448702697669,
                    random_strength=5.31079713814424e-05,
                    bagging_temperature=0.6697368953356237,
                    verbose=False,
                    random_state=42,
                ),
            },
        }

    @staticmethod
    def get_random_forest_classifier(
        **params,  # noqa
    ) -> RandomForestClassifier:
        return RandomForestClassifier(**params)

    @staticmethod
    def get_catboost_classifier(
        **params,  # noqa
    ) -> CatBoostClassifier:
        return CatBoostClassifier(**params)

    def classifier(self, company: str, target: str) -> any:  # type: ignore
        if (
            company not in self.classifier_mapping
            or target not in self.classifier_mapping[company]
        ):
            raise ValueError(
                f"No classifier found for company '{company}' and target '{target}'"
            )
        return self.classifier_mapping[company][target]

    evaluation_metric = "f1"

    class_imbalance_thresholds = {
        "severe": 0.1,
        "significant": 0.2,
        "slight": 0.3,
    }
