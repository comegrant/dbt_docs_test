from pydantic import BaseModel
from reci_pick.shared_configs import get_recipe_numeric_features_config


class CompanyPredictConfigs(BaseModel):
    user_profile_start_yyyyww: int = 202401
    top_n_per_concept: int = 8
    top_n_per_user: int = 5
    look_back_weeks: int = 24
    pooling_method: str = "mean"
    similarity_threshold: float = 0.91
    is_map_similar_recipes: bool = True
    repeated_purchase_bonus_factor: float = 0.25
    alpha: float = -1.0
    high_frequency_penalization_factor: float = 0.1
    model_uri: dict = {"dev": None, "test": None, "prod": None}
    preprocessor_uri: dict = {"dev": None, "test": None, "prod": None}
    num_user_chunks: int = 20
    recipe_numeric_features: list[str] = [
        "normalize__cooking_time_mean",
        "normalize__number_of_recipe_steps",
        "normalize__number_of_taxonomies",
        "normalize__number_of_ingredients",
        "has_vegetarian_taxonomy",
        "has_quick_and_easy_taxonomy",
        "has_low_calorie_taxonomy",
        "onehot__recipe_main_ingredient_name_english_Beef",
        "onehot__recipe_main_ingredient_name_english_Fish",
        "onehot__recipe_main_ingredient_name_english_Pork",
        "onehot__recipe_main_ingredient_name_english_Poultry",
    ]


def get_company_predict_configs(company_code: str) -> CompanyPredictConfigs:
    if company_code == "AMK":
        return amk_predict_configs
    elif company_code == "LMK":
        return lmk_predict_configs
    elif company_code == "GL":
        return gl_predict_configs
    elif company_code == "RT":
        return rt_predict_configs
    else:
        raise ValueError(f"{company_code} is not a valid company code!")


def get_model_uri(company_code: str, artifact_type: str) -> dict:
    if artifact_type == "model":
        return {
            "dev": f"models:/dev.mloutputs.reci_pick_{company_code.lower()}@champion",
            "test": f"models:/test.mloutputs.reci_pick_{company_code.lower()}@champion",
            "prod": f"models:/prod.mloutputs.reci_pick_{company_code.lower()}@champion",
        }
    elif artifact_type == "preprocessor":
        return {
            "dev": f"models:/dev.mloutputs.reci_pick_preprocessor_{company_code.lower()}@champion",
            "test": f"models:/test.mloutputs.reci_pick_preprocessor_{company_code.lower()}@champion",
            "prod": f"models:/prod.mloutputs.reci_pick_preprocessor_{company_code.lower()}@champion",
        }


amk_predict_configs = CompanyPredictConfigs(
    model_uri=get_model_uri(company_code="AMK", artifact_type="model"),
    preprocessor_uri=get_model_uri(company_code="AMK", artifact_type="preprocessor"),
    num_user_chunks=10,
    recipe_numeric_features=get_recipe_numeric_features_config("AMK"),
)


gl_predict_configs = CompanyPredictConfigs(
    model_uri=get_model_uri(company_code="GL", artifact_type="model"),
    preprocessor_uri=get_model_uri(company_code="GL", artifact_type="preprocessor"),
    num_user_chunks=20,
    recipe_numeric_features=get_recipe_numeric_features_config("GL"),
)


rt_predict_configs = CompanyPredictConfigs(
    model_uri=get_model_uri(company_code="RT", artifact_type="model"),
    preprocessor_uri=get_model_uri(company_code="RT", artifact_type="preprocessor"),
    num_user_chunks=10,
    recipe_numeric_features=get_recipe_numeric_features_config("RT"),
)

lmk_predict_configs = CompanyPredictConfigs(
    model_uri=get_model_uri(company_code="LMK", artifact_type="model"),
    preprocessor_uri=get_model_uri(company_code="LMK", artifact_type="preprocessor"),
    num_user_chunks=20,
    recipe_numeric_features=get_recipe_numeric_features_config("LMK"),
)
