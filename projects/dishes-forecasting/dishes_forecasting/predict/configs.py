from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: dict[str, str]


gl_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/172c141f1d984d5c9719bd49c2f0d7f5/dishes_forecasting_GL",
        "test": "runs:/dd8d57c749b446d8a336cae94a22ecd7/dishes_forecasting_GL",
    }
)

amk_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/e6891dfd222342b1b931339a3af6ed4d/dishes_forecasting_AMK",
        "test": "runs:/839370005b754de0ad03afd6798c73bd/dishes_forecasting_AMK",
    }
)


rt_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/c4ac7d1e087c4b22bbb2eabc32c8ac90/dishes_forecasting_RT",
        "test": "runs:/7c9c948cfd0b444b9b6e2b81cb92e0f3/dishes_forecasting_RT",
    }
)

lmk_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/9d4390ad01c44afebd86383b380c97a4/dishes_forecasting_LMK",
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
