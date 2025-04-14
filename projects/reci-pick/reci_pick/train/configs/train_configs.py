from pydantic import BaseModel
from reci_pick.shared_configs import get_recipe_numeric_features_config


class CompanyTrainConfigs(BaseModel):
    train_start_yyyyww: int
    impute_and_normalize_columns: list[str] = [
        "cooking_time_mean",
        "number_of_recipe_steps",
        "number_of_taxonomies",
        "number_of_ingredients",
    ]

    impute_and_one_hot_columns: list[str] = [
        "recipe_main_ingredient_name_english",
        "recipe_difficulty_level_id",
    ]

    impute_only_columns: list[str] = [
        "has_chefs_favorite_taxonomy",
        "has_quick_and_easy_taxonomy",
        "has_vegetarian_taxonomy",
        "has_low_calorie_taxonomy",
    ]
    recipe_numeric_features: list[str] = [
        "cooking_time_mean",
        "number_of_recipe_steps",
        "number_of_taxonomies",
        "number_of_ingredients",
        "has_vegetarian_taxonomy",
        "has_quick_and_easy_taxonomy",
        "has_low_calorie_taxonomy",
        "recipe_main_ingredient_name_english_Beef",
        "recipe_main_ingredient_name_english_Fish",
        "recipe_main_ingredient_name_english_Pork",
        "recipe_main_ingredient_name_english_Poultry",
    ]
    training_size: int = 300000
    num_validation_weeks: int = 6
    num_test_users: int = 2000


def get_company_train_configs(company_code: str) -> CompanyTrainConfigs:
    if company_code == "AMK":
        return amk_train_configs
    elif company_code == "LMK":
        return lmk_train_configs
    elif company_code == "GL":
        return gl_train_configs
    elif company_code == "RT":
        return rt_train_configs
    else:
        raise ValueError(f"{company_code} is not a valid company code!")


amk_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202401,
    training_size=300000,
    num_validation_weeks=6,
    num_test_users=2000,
    recipe_numeric_features=get_recipe_numeric_features_config("AMK"),
)
gl_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202401,
    training_size=300000,
    num_validation_weeks=6,
    num_test_users=2000,
    recipe_numeric_features=get_recipe_numeric_features_config("GL"),
)

lmk_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202401,
    training_size=300000,
    num_validation_weeks=6,
    num_test_users=2000,
    recipe_numeric_features=get_recipe_numeric_features_config("LMK"),
)

rt_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202401,
    training_size=300000,
    num_validation_weeks=6,
    num_test_users=2000,
    recipe_numeric_features=get_recipe_numeric_features_config("RT"),
)
