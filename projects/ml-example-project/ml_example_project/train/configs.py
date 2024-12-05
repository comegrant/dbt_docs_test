from pydantic import BaseModel


class CompanyTrainConfigs(BaseModel):
    train_start_yyyyww: int
    train_end_yyyyww: int
    model_params: dict


def get_company_train_configs(
    company_code: str
) -> CompanyTrainConfigs:
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
    train_start_yyyyww=202301,
    train_end_yyyyww=202451,
    model_params={
        "n_estimators": 100
    }
)

lmk_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202301,
    train_end_yyyyww=202451,
    model_params={
        "n_estimators": 100
    }
)

gl_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202301,
    train_end_yyyyww=202451,
    model_params={
        "n_estimators": 100
    }
)

rt_train_configs = CompanyTrainConfigs(
    train_start_yyyyww=202401,
    train_end_yyyyww=202451,
    model_params={
        "n_estimators": 100
    }
)


class FeatureLookUpConfig(BaseModel):
    features_container: str = "mlfeatures"
    feature_table_name: str
    primary_keys: list[str] = []
    feature_columns: list[str] = []
    exclude_in_training_set: list[str] = []


recipes_lookup = FeatureLookUpConfig(
    feature_table_name="ft_ml_recipes",
    primary_keys=[
        "recipe_id",
    ],
    feature_columns=[
        "cooking_time_from",
        "cooking_time_to",
        "number_of_taxonomies",
        "number_of_recipe_steps",
        "number_of_ingredients"
    ],
    exclude_in_training_set=[
        "recipe_id",
    ]
)

feature_lookup_config_list = [
    recipes_lookup,
]
