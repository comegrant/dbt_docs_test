from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: dict[str, str]


gl_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/172c141f1d984d5c9719bd49c2f0d7f5/dishes_forecasting_GL",
        "test": "runs:/027d6f026e7f4552b7f68944dc16d8a8/dishes_forecasting_GL",
    }
)

amk_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/e6891dfd222342b1b931339a3af6ed4d/dishes_forecasting_AMK",
        "test": "runs:/541214cc0adb4674afa3e4fbf2c7b2b0/dishes_forecasting_AMK",
    }
)


rt_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/c4ac7d1e087c4b22bbb2eabc32c8ac90/dishes_forecasting_RT",
        "test": "runs:/010ce28668c44ef49776c0483f7892c8/dishes_forecasting_RT",
    }
)

lmk_configs = PredictConfigs(
    model_uri={
        "dev": "runs:/9d4390ad01c44afebd86383b380c97a4/dishes_forecasting_LMK",
        "test": "runs:/b1fb66c025d041bda812f5b983c44ccd/dishes_forecasting_LMK",
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
