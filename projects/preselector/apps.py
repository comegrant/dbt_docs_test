from chef.deploy import Apps, CommonSecrets, ContainerResources, EnvConfig, StreamlitApp
from pydantic import SecretStr
from pydantic_settings import BaseSettings
from streamlit_apps import debug_app


class RedisConfig(BaseSettings):
    redis_url: SecretStr


apps = Apps(
    debug=StreamlitApp(
        main_app=debug_app,
        secrets=[CommonSecrets, RedisConfig],
        env_vars=EnvConfig(
            test={
                "UC_ENV": "prod",
                "DATALAKE_ENV": "prod",
                "auth_redirect_uri": "https://preselector-debug-dev.purplehill-5812464f.northeurope.azurecontainerapps.io/",
            },
            prod={
                "UC_ENV": "prod",
                "DATALAKE_ENV": "prod",
                "auth_redirect_uri": "https://preselector-debug-prod.delightfulpond-81d463af.northeurope.azurecontainerapps.io",
            },
        ),
        resources=ContainerResources(cpu=1, memory="2Gi"),
    ),
)
