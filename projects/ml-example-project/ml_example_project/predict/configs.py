from pydantic import BaseModel


class CompanyPredictConfigs(BaseModel):
    model_uri: str


def get_company_predict_configs(company_code: str, env: str) -> CompanyPredictConfigs:
    if company_code == "AMK":
        return CompanyPredictConfigs(model_uri=f"models:/{env}.mloutputs.ml_example_project_amk@champion")
    elif company_code == "LMK":
        return CompanyPredictConfigs(model_uri=f"models:/{env}.mloutputs.ml_example_project_lmk@champion")
    elif company_code == "GL":
        return CompanyPredictConfigs(model_uri=f"models:/{env}.mloutputs.ml_example_project_gl@champion")
    elif company_code == "RT":
        return CompanyPredictConfigs(model_uri=f"models:/{env}.mloutputs.ml_example_project_rt@champion")
    else:
        raise ValueError(f"{company_code} is not a valid company code!")
