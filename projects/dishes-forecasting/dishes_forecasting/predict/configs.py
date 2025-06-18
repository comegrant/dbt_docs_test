from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: dict[str, str]


gl_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_gl@champion",
        "test": "models:/test.mloutputs.dishes_forecasting_gl@champion",
        "prod": "models:/prod.mloutputs.dishes_forecasting_gl@champion",
    }
)

amk_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_amk@champion",
        "test": "models:/test.mloutputs.dishes_forecasting_amk@champion",
        "prod": "models:/prod.mloutputs.dishes_forecasting_amk@champion",
    }
)


rt_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_rt@champion",
        "test": "models:/test.mloutputs.dishes_forecasting_rt@champion",
        "prod": "models:/prod.mloutputs.dishes_forecasting_rt@champion",
    }
)

lmk_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_lmk@champion",
        "test": "models:/test.mloutputs.dishes_forecasting_lmk@champion",
        "prod": "models:/prod.mloutputs.dishes_forecasting_lmk@champion",
    }
)


def get_configs(company_code: str) -> PredictConfigs:
    if company_code == "GL":
        return gl_configs
    elif company_code == "AMK":
        return amk_configs
    elif company_code == "LMK":
        return lmk_configs
    elif company_code == "RT":
        return rt_configs
    else:
        raise ValueError(f"Prediction configs for {company_code} does not exist.")
