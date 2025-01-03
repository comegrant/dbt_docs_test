from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: dict[str, str]


gl_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_gl/3",
        "test": "runs:/dd8d57c749b446d8a336cae94a22ecd7/dishes_forecasting_GL",
    }
)

amk_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_amk/2",
        "test": "runs:/839370005b754de0ad03afd6798c73bd/dishes_forecasting_AMK",
    }
)


rt_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_rt/4",
        "test": "runs:/7c9c948cfd0b444b9b6e2b81cb92e0f3/dishes_forecasting_RT",
    }
)

lmk_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_lmk/3",
        "test": "runs:/7c1c4cebb7a540c98ea83854f10fb3f9/dishes_forecasting_LMK",
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
