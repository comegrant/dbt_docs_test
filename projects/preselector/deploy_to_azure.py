import logging
from time import sleep

from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupIdentity,
    ContainerGroupRestartPolicy,
    ContainerNetworkProtocol,
    ContainerPort,
    EnvironmentVariable,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
    UserAssignedIdentities,
)
from cheffelo_logging.logging import DataDogConfig, DataDogStatsdConfig
from preselector.process_stream_settings import ProcessStreamSettings
from pydantic import SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class DeploySettings(BaseSettings):
    docker_registry_server: str
    docker_registry_username: str
    docker_registry_password: SecretStr

    user_assigned_identity_resource_id: str
    subscription_id: str


class RuntimeEnvs(BaseSettings):
    datalake_env: str

    # Used for datadog logging tag
    env: str


def datadog_agent_container(config: DataDogConfig, name: str) -> Container:
    image = "gcr.io/datadoghq/agent:latest"
    return Container(
        name=name,
        image=image,
        environment_variables=[
            EnvironmentVariable(name="DD_API_KEY", secure_value=config.datadog_api_key),
            EnvironmentVariable(name="DD_SITE", value=config.datadog_site),
            EnvironmentVariable(name="DD_HOSTNAME", value=config.datadog_service_name),
            EnvironmentVariable(name="DD_SERVICE", value=config.datadog_service_name),
        ],
        ports=[ContainerPort(port=8125, protocol=ContainerNetworkProtocol.UDP)],
        resources=ResourceRequirements(
            requests=ResourceRequests(
                memory_in_gb=1.0,
                cpu=1.0,
                gpu=None
            )
        )
    )


def deploy_preselector(
    name: str,
    company: str,
    service_bus_namespace: str,
    env: str,
    image: str,
    image_tag: str,
    resource_group: str,
) -> None:
    from dotenv import load_dotenv
    load_dotenv(".env")

    deploy_settings = DeploySettings(
        docker_registry_server="bhregistry.azurecr.io",
        docker_registry_username="bhregistry"
    ) # type: ignore[reportGeneralTypeIssues]

    client = ContainerInstanceManagementClient(
        credential=DefaultAzureCredential(),
        subscription_id=deploy_settings.subscription_id
    )
    dd_config = DataDogConfig( # type: ignore[reportGeneralTypeIssues]
        datadog_service_name="preselector",
        datadog_tags=f"subscription:{company},env:{env},image-tag:{image_tag}",
    )
    datadog_host = "datadog-agent"

    def process_container(
        topic_name: str,
        container_name: str,
        batch_size: int
    ) -> Container:

        if env != "prod":
            assert env in deploy_settings.user_assigned_identity_resource_id

        environment_variables: list[EnvironmentVariable] = []

        settings = [
            ProcessStreamSettings( # type: ignore[reportGeneralTypeIssues]
                service_bus_subscription_name=company,
                service_bus_namespace=service_bus_namespace,
                service_bus_should_write=True,
                service_bus_connection_string=None,
                service_bus_request_topic_name=topic_name,
                service_bus_request_size=batch_size
            ),
            DataDogStatsdConfig(
                datadog_host="127.0.0.1"
            ),
            DataDogConfig( # type: ignore[reportGeneralTypeIssues]
                datadog_service_name="preselector",
                datadog_tags=dd_config.datadog_tags + f",topic:{topic_name}"
            ),
            RuntimeEnvs(
                datalake_env=env,
                env=env
            )
        ]

        for setting in settings:
            for key, value in setting.model_dump().items():
                if value is None:
                    continue

                if isinstance(value, SecretStr):
                    environment_variables.append(
                        EnvironmentVariable(
                            name=key.upper(),
                            secure_value=value.get_secret_value()
                        )
                    )
                else:
                    environment_variables.append(
                        EnvironmentVariable(
                            name=key.upper(),
                            value=value
                        )
                    )


        return Container(
            name=container_name,
            image=image,
            environment_variables=environment_variables,
            ports=[],
            command=[ "/bin/bash", "-c", "python -m preselector.process_stream" ],
            resources=ResourceRequirements(
                requests=ResourceRequests(
                    memory_in_gb=7,
                    cpu=1.0,
                    gpu=None
                )
            )
        )

    group = ContainerGroup(
        containers=[
            datadog_agent_container(dd_config, name=datadog_host),
            process_container(
                topic_name="priority-deviation-request",
                container_name=f"{name}-live",
                batch_size=10
            ),
            process_container(
                topic_name="deviation-request",
                container_name=f"{name}-batch",
                batch_size=50
            )
        ],
        os_type=OperatingSystemTypes.LINUX,
        restart_policy=ContainerGroupRestartPolicy.ON_FAILURE,
        identity=ContainerGroupIdentity(
            type="UserAssigned",
            user_assigned_identities={
                deploy_settings.user_assigned_identity_resource_id: UserAssignedIdentities()
            }
        ),
        image_registry_credentials=[
            ImageRegistryCredential(
                server=deploy_settings.docker_registry_server,
                password=deploy_settings.docker_registry_password.get_secret_value(),
                username=deploy_settings.docker_registry_username
            )
        ],
        location="northeurope"
    )

    logger.info("deleting resource")
    poller = client.container_groups.begin_delete(
       resource_group_name=resource_group, container_group_name=name)

    poller.wait()
    logger.info("Waiting for 10 secs")
    sleep(10)

    while not poller.done():
        logger.info("Waiting for delete")

    logger.info("creating resource")
    client.container_groups.begin_create_or_update(
        resource_group_name=resource_group,
        container_group_name=name,
        container_group=group
    )

def deploy_all(tag: str, env: str) -> None:
    company_names = [
        "godtlevert",
        "adams",
        "linas",
        "retnemt",
    ]

    bus_namespace = {
        "test": "gg-deviation-service-qa.servicebus.windows.net",
        "prod": "gg-deviation-service-prod.servicebus.windows.net"
    }

    for company in company_names:

        logger.info(company)
        name = f"preselector-{company}-worker"
        if company == "godtlevert":
            name = "preselector-gl-worker"

        name += f"-{env}"
        logger.info(name)

        deploy_preselector(
            name=name,
            company=company,
            service_bus_namespace=bus_namespace[env],
            env=env,
            image=f"bhregistry.azurecr.io/preselector:{tag}",
            image_tag=tag,
            resource_group=f"rg-chefdp-{env}",
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    deploy_all(tag="dev-latest", env="test")
