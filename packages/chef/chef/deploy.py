import logging
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Protocol

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

    should_deploy_datadog_agent: bool

    startup_command: list[str]
    exposed_at_port: int | None = None

    env_vars: dict[str, str] | None = None
    secrets: type[BaseSettings] | list[type[BaseSettings]] | None = None

    container_registry_url: str = "bhregistry.azurecr.io"
    resources: ContainerResources = field(default_factory=lambda: ContainerResources(cpu=0.5, memory="1Gi"))
    ip_rules: list[IpSecurityRestrictionRule] | None = None

    location: str = "northeurope"
    subscription_id: str = "7c54c7c3-c54c-44bd-969c-440ecef1d917"


class AllowIpAddress:
    # DATA-1736
    norway_vpn = IpSecurityRestrictionRule(name="Norway VPN", ip_address_range="185.7.138.205/32", action="Allow")
    norway_office = IpSecurityRestrictionRule(
        name="Norway Office", ip_address_range="109.74.178.186/32", action="Allow"
    )
    norway_sb2 = IpSecurityRestrictionRule(
        name="Norway SB2 Storage", ip_address_range="77.40.143.14/32", action="Allow"
    )

    sweden_vpn = IpSecurityRestrictionRule(name="Sweden VPN", ip_address_range="194.236.49.120/32", action="Allow")
    sweden_main_office = IpSecurityRestrictionRule(
        name="Sweden Main Office", ip_address_range="84.55.74.180/32", action="Allow"
    )
    sweden_gothenburg_office = IpSecurityRestrictionRule(
        name="Sweden Gothenburg Office", ip_address_range="62.20.23.147/32", action="Allow"
    )

    danmark_office = IpSecurityRestrictionRule(name="Danmark Office", ip_address_range="83.94.96.6/32", action="Allow")
    danmark_vpn = IpSecurityRestrictionRule(name="Danmark VPN", ip_address_range="62.20.23.147/32", action="Allow")

    syone = IpSecurityRestrictionRule(name="Syone", ip_address_range="148.69.136.81/32", action="Allow")


def default_firewall() -> list[IpSecurityRestrictionRule]:
    return [
        AllowIpAddress.norway_vpn,
        AllowIpAddress.norway_office,
        AllowIpAddress.norway_sb2,
        AllowIpAddress.sweden_vpn,
        AllowIpAddress.sweden_main_office,
        AllowIpAddress.sweden_gothenburg_office,
        AllowIpAddress.danmark_vpn,
        AllowIpAddress.danmark_office,
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
            external=True,
            target_port=config.exposed_at_port,
            transport="auto",
            ip_security_restrictions=config.ip_rules or default_firewall(),
        )

    containers = [streamlit_container]

    if config.should_deploy_datadog_agent:
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
        containers.append(datadog_sidecar)

    app = ContainerApp(
        location=config.location,
        tags=tags,
        managed_environment_id=env.id,
        configuration=container_config,
        template=Template(
            containers=containers,
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


class App(Protocol):
    def exposed_at_port(self) -> int | None: ...

    def startup_commands(self) -> list[str]: ...

    def used_env_vars(self, env: str) -> dict[str, str]: ...

    def resource_config(self) -> ContainerResources: ...

    def used_secrets(self) -> list[type[BaseSettings]] | None: ...

    def should_add_datadog_agent(self) -> bool: ...

    def used_ip_rules(self) -> list[IpSecurityRestrictionRule] | None: ...


@dataclass
class StreamlitApp:
    main_app: str

    secrets: type[BaseSettings] | list[type[BaseSettings]] | None = None
    env_vars: EnvConfig | dict[str, str] | None = None

    ip_rules: list[IpSecurityRestrictionRule] | None = None
    resources: ContainerResources = field(default_factory=lambda: ContainerResources(cpu=0.5, memory="1Gi"))

    def exposed_at_port(self) -> int | None:
        return 8501

    def startup_commands(self) -> list[str]:
        command = f"python -m streamlit run {self.main_app} --server.fileWatcherType none"
        return ["/bin/sh", "-c", command]

    def used_env_vars(self, env: str) -> dict[str, str]:
        used_env_vars = {}
        if isinstance(self.env_vars, dict):
            used_env_vars = self.env_vars

        elif isinstance(self.env_vars, EnvConfig):
            if env == "prod":
                used_env_vars = self.env_vars.prod
            else:
                used_env_vars = self.env_vars.test

        return used_env_vars

    def resource_config(self) -> ContainerResources:
        return self.resources

    def used_secrets(self) -> list[type[BaseSettings]] | None:
        if self.secrets is None:
            return None
        if isinstance(self.secrets, list):
            return self.secrets
        else:
            return [self.secrets]

    def should_add_datadog_agent(self) -> bool:
        return True

    def used_ip_rules(self) -> list[IpSecurityRestrictionRule] | None:
        return self.ip_rules


@dataclass
class GenericApp:
    command: str
    port: int | None
    secrets: type[BaseSettings] | list[type[BaseSettings]] | None = None
    env_vars: EnvConfig | dict[str, str] | None = None
    ip_rules: list[IpSecurityRestrictionRule] | None = None

    resources: ContainerResources = field(default_factory=lambda: ContainerResources(cpu=0.5, memory="1Gi"))
    should_deploy_datadog_agent: bool = field(default=True)

    def exposed_at_port(self) -> int | None:
        return self.port

    def startup_commands(self) -> list[str]:
        return ["/bin/sh", "-c", self.command]

    def used_env_vars(self, env: str) -> dict[str, str]:
        used_env_vars = {}
        if isinstance(self.env_vars, dict):
            used_env_vars = self.env_vars

        elif isinstance(self.env_vars, EnvConfig):
            if env == "prod":
                used_env_vars = self.env_vars.prod
            else:
                used_env_vars = self.env_vars.test

        return used_env_vars

    def resource_config(self) -> ContainerResources:
        return self.resources

    def used_secrets(self) -> list[type[BaseSettings]] | None:
        if self.secrets is None:
            return None
        if isinstance(self.secrets, list):
            return self.secrets
        else:
            return [self.secrets]

    def should_add_datadog_agent(self) -> bool:
        return self.should_deploy_datadog_agent

    def used_ip_rules(self) -> list[IpSecurityRestrictionRule] | None:
        return self.ip_rules


class Apps:
    applications: dict[str, App]
    docker_image: str | None

    def __init__(self, docker_image: str | None = None, **applications: App):
        self.applications = applications
        self.docker_image = docker_image

    def config_for(self, project_name: str, name: str, env: str = "test") -> DeployConfig:
        app = self.applications[name]

        return DeployConfig(
            app_name=f"{project_name}-{name}-{env}".replace("_", "-").lower(),
            container_image=self.docker_image or "",
            ip_rules=app.used_ip_rules(),
            should_deploy_datadog_agent=app.should_add_datadog_agent(),
            startup_command=app.startup_commands(),
            exposed_at_port=app.exposed_at_port(),
            env_vars=app.used_env_vars(env),
            secrets=app.used_secrets(),
            resources=app.resource_config(),
        )
