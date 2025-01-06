from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: dict[str, str]


gl_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_gl/3",
        "test": "models:/test.mloutputs.dishes_forecasting_gl/3",
    }
)

amk_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_amk/2",
        "test": "models:/test.mloutputs.dishes_forecasting_amk/3",
    }
)


rt_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_rt/4",
        "test": "models:/test.mloutputs.dishes_forecasting_rt/3",
    }
)

lmk_configs = PredictConfigs(
    model_uri={
        "dev": "models:/dev.mloutputs.dishes_forecasting_lmk/3",
        "test": "models:/test.mloutputs.dishes_forecasting_lmk/4",
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
