from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    EnvironmentVariable,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
)
from cheffelo_logging.logging import DataDogConfig
from preselector.process_stream_settings import ProcessStreamSettings
from pydantic import SecretStr
from pydantic_settings import BaseSettings


class DeploySettings(BaseSettings):
    docker_registry_server: str
    docker_registry_username: str
    docker_registry_password: SecretStr



def deploy_preselector(
    company: str,
    service_bus_namespace: str,
    image: str
) -> None:

    from dotenv import load_dotenv

    load_dotenv(".env")

    client = ContainerInstanceManagementClient(
        credential=DefaultAzureCredential(),
        subscription_id="7c54c7c3-c54c-44bd-969c-440ecef1d917"
    )

    docker_settings = DeploySettings() # type: ignore[reportGeneralTypeIssues]

    environment_variables: list[EnvironmentVariable] = []

    settings = [
        ProcessStreamSettings( # type: ignore[reportGeneralTypeIssues]
            service_bus_subscription_name=company,
            service_bus_namespace=service_bus_namespace,
            service_bus_should_write=True,
            service_bus_connection_string=None
        ),
        DataDogConfig( # type: ignore[reportGeneralTypeIssues]
            datadog_service_name="preselector",
            datadog_tags=f"env:qa,subscription:{company}",
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


    worker = Container(
        name=f"preselector-{company}-worker",
        image=image,
        environment_variables=environment_variables,
        ports=[],
        command=[ "/bin/bash", "-c", "python -m preselector.process_stream" ],
        resources=ResourceRequirements(
            requests=ResourceRequests(
                memory_in_gb=8,
                cpu=1.0,
                gpu=None
            )
        )
    )


    group = ContainerGroup(
        containers=[worker],
        os_type=OperatingSystemTypes.LINUX,
        restart_policy=ContainerGroupRestartPolicy.ON_FAILURE,
        image_registry_credentials=[
            ImageRegistryCredential(
                server=docker_settings.docker_registry_server,
                password=docker_settings.docker_registry_password.get_secret_value(),
                username=docker_settings.docker_registry_username
            )
        ],
        location="northeurope"
    )

    client.container_groups.begin_create_or_update(
        resource_group_name="gg-analytics-backend-qa",
        container_group_name=worker.name,
        container_group=group
    )


if __name__ == "__main__":
    deploy_preselector(
        company="linas",
        service_bus_namespace="gg-deviation-service-bicep-qa.servicebus.windows.net",
        image="bhregistry.azurecr.io/preselector:dev-latest",
    )
