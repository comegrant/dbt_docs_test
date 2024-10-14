from pydantic import BaseModel


class PredictConfigs(BaseModel):
    model_uri: str


gl_configs = PredictConfigs(
    model_uri="runs:/034d770e906c4cbd8a99aa392d0b19f6/dishes_pipeline_test_GL"
)


def get_configs(company_code: str) -> PredictConfigs:
    if company_code == "GL":
        return gl_configs
