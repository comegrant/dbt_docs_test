
from pydantic import BaseModel


class CompanyPredictConfigs(BaseModel):
    model_uri: str


def get_company_predict_configs(
    company_code: str
) -> CompanyPredictConfigs:
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


amk_predict_configs = CompanyPredictConfigs(
    model_uri='runs:/d69a11fda38b434aaec4db2377fb2cea/test'
)

lmk_predict_configs = CompanyPredictConfigs(
    model_uri='runs:/d69a11fda38b434aaec4db2377fb2cea/test'
)

gl_predict_configs = CompanyPredictConfigs(
    model_uri='runs:/8861c63c35b14c378a33078ae5a6c1e9/test'
)

rt_predict_configs = CompanyPredictConfigs(
    model_uri='runs:/d69a11fda38b434aaec4db2377fb2cea/test'
)
