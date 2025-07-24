import logging
from contextlib import suppress
from dataclasses import dataclass, field

from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import (
    Configuration,
    Container,
    ContainerApp,
    ContainerResources,
    EnvironmentVar,
    Ingress,
    IpSecurityRestrictionRule,
    ManagedEnvironment,
    RegistryCredentials,
    Scale,
    Secret,
    Template,
    VnetConfiguration,
)
from pydantic import SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class CommonSecrets(BaseSettings):
    datalake_service_account_name: str
    datalake_storage_account_key: SecretStr

    databricks_token: SecretStr
    databricks_host: str

    auth_client_id: str
    auth_secret: SecretStr


class DatadogSecret(BaseSettings):
    datadog_api_key: SecretStr


class DockerSecrets(BaseSettings):
    docker_password: SecretStr


@dataclass
class DeployConfig:
    app_name: str
    container_image: str

    startup_command: list[str]
    exposed_at_port: int | None = None

    env_vars: dict[str, str] | None = None
    secrets: type[BaseSettings] | list[type[BaseSettings]] | None = None

    container_registry_url: str = "bhregistry.azurecr.io"
    resources: ContainerResources = field(default_factory=lambda: ContainerResources(cpu=0.5, memory="1Gi"))

    location: str = "northeurope"
    subscription_id: str = "7c54c7c3-c54c-44bd-969c-440ecef1d917"


def firewall() -> list[IpSecurityRestrictionRule]:
    return [
        IpSecurityRestrictionRule(name="VPN Norway", ip_address_range="185.7.138.205/32", action="Allow"),
        IpSecurityRestrictionRule(name="Office Norway", ip_address_range="109.74.178.186/32", action="Allow"),
    ]


async def deploy(
    config: DeployConfig,
    environment: str = "test",
) -> str | None:
    from key_vault import key_vault

    vault = key_vault(environment)

    resource_group = "gg-analytics-backend-qa"

    if environment == "prod":
        resource_group = "gg-analytics-backend-prod"

    credential = DefaultAzureCredential()
    app_client = ContainerAppsAPIClient(credential, config.subscription_id)

    tags = {"env": environment, "managed_by": "sous-chef-cli", "app": config.app_name, "team": "data"}

    managed_env = ManagedEnvironment(
        location=config.location,
        tags=tags,
        vnet_configuration=VnetConfiguration(internal=False),
    )

    logger.info("Creating a managed env with.")

    env_name = f"{config.app_name}-env"
    env_poller = app_client.managed_environments.begin_create_or_update(resource_group, env_name, managed_env)
    env = env_poller.result()

    env_vars = config.env_vars or {}

    if "UC_ENV" not in env_vars:
        env_vars["UC_ENV"] = environment
    if "DATALAKE_ENV" not in env_vars:
        env_vars["DATALAKE_ENV"] = environment

    secrets: list[Secret] = []
    env_vals: list[EnvironmentVar] = [EnvironmentVar(name=key, value=value) for key, value in env_vars.items()]
    secret_settings: list[type[BaseSettings]] = []
    if config.secrets:
        if not isinstance(config.secrets, list):
            secret_settings = [config.secrets]
        else:
            secret_settings = config.secrets
    else:
        secret_settings = [CommonSecrets]

    if DockerSecrets not in secret_settings:
        secret_settings.append(DockerSecrets)
    if DatadogSecret not in secret_settings:
        secret_settings.append(DatadogSecret)

    for setting in secret_settings:
        vals = await vault.load(setting)
        for key, value in vals.model_dump().items():
            secret_name = key.replace("_", "-")

            if isinstance(value, SecretStr):
                secrets.append(Secret(name=secret_name, value=value.get_secret_value()))
            else:
                secrets.append(Secret(name=secret_name, value=value))

            env_vals.append(EnvironmentVar(name=key.upper(), secret_ref=secret_name))

    streamlit_container = Container(
        name=config.app_name,
        image=config.container_image,
        command=config.startup_command,
        env=[EnvironmentVar(name="env", value=environment), *env_vals],
        resources=config.resources,
    )

    container_config = Configuration(
        ingress=None,
        registries=[
            RegistryCredentials(
                server=config.container_registry_url, username="bhregistry", password_secret_ref="docker-password"
            )
        ],
        secrets=[*secrets],
    )

    if config.exposed_at_port:
        container_config.ingress = Ingress(
            external=True, target_port=config.exposed_at_port, transport="auto", ip_security_restrictions=firewall()
        )

    datadog_sidecar = Container(
        name="datadog",
        image="gcr.io/datadoghq/agent:latest",
        env=[
            EnvironmentVar(name="DD_API_KEY", secret_ref="datadog-api-key"),
            EnvironmentVar(name="DD_SITE", value="datadoghq.eu"),
            EnvironmentVar(name="DD_HOSTNAME", value=config.app_name),
            EnvironmentVar(name="DD_SERVICE", value=config.app_name),
        ],
        resources=ContainerResources(cpu=0.5, memory="1Gi"),
    )

    app = ContainerApp(
        location=config.location,
        tags=tags,
        managed_environment_id=env.id,
        configuration=container_config,
        template=Template(
            containers=[streamlit_container, datadog_sidecar],
            scale=Scale(min_replicas=0, max_replicas=1),
        ),
    )

    app_poller = app_client.container_apps.begin_create_or_update(resource_group, config.app_name, app)
    created_app = app_poller.result()

    with suppress(AttributeError):
        return f"https://{created_app.configuration.ingress.fqdn}"  # type: ignore


@dataclass
class EnvConfig:
    test: dict[str, str]
    prod: dict[str, str]


@dataclass
class StreamlitApp:
    main_app: str

    secrets: type[BaseSettings] | list[type[BaseSettings]] | None = None
    env_vars: EnvConfig | dict[str, str] | None = None

    resources: ContainerResources = field(default_factory=lambda: ContainerResources(cpu=0.5, memory="1Gi"))


class Apps:
    applications: dict[str, StreamlitApp]
    docker_image: str | None

    def __init__(self, docker_image: str | None = None, **applications: StreamlitApp):
        self.applications = applications
        self.docker_image = docker_image

    def config_for(self, project_name: str, name: str, env: str = "test") -> DeployConfig:
        streamlit_app = self.applications[name]
        command = f"python -m streamlit run {streamlit_app.main_app} --server.fileWatcherType none"

        used_env_vars: dict[str, str] = {}

        if isinstance(streamlit_app.env_vars, dict):
            used_env_vars = streamlit_app.env_vars

        elif isinstance(streamlit_app.env_vars, EnvConfig):
            if env == "prod":
                used_env_vars = streamlit_app.env_vars.prod
            else:
                used_env_vars = streamlit_app.env_vars.test

        return DeployConfig(
            app_name=f"{project_name}-{name}-{env}".replace("_", "-").lower(),
            container_image=self.docker_image or "",
            startup_command=["/bin/sh", "-c", command],
            exposed_at_port=8501,
            env_vars=used_env_vars,
            secrets=streamlit_app.secrets,
            resources=streamlit_app.resources,
        )
