from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: str


gl_configs = PredictConfigs(
    model_uri="runs:/19bcae6424464d4a9959dc3cc98eb214/dishes_pipeline_workflow_GL"
)


def get_configs(company_code: str) -> PredictConfigs:
    if company_code == "GL":
        return gl_configs
