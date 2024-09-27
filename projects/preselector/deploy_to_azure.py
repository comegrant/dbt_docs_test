
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupIdentity,
    ContainerGroupRestartPolicy,
    EnvironmentVariable,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
    UserAssignedIdentities,
)
from cheffelo_logging.logging import DataDogConfig
from preselector.process_stream_settings import ProcessStreamSettings
from pydantic import SecretStr
from pydantic_settings import BaseSettings


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




def deploy_preselector(
    name: str,
    company: str,
    service_bus_namespace: str,
    env: str,
    image: str,
    resource_group: str
) -> None:
    from dotenv import load_dotenv
    load_dotenv(".env")

    deploy_settings = DeploySettings(
        docker_registry_server="bhregistry.azurecr.io",
        docker_registry_username="bhregistry"
    ) # type: ignore[reportGeneralTypeIssues]

    assert env in deploy_settings.user_assigned_identity_resource_id

    client = ContainerInstanceManagementClient(
        credential=DefaultAzureCredential(),
        subscription_id=deploy_settings.subscription_id
    )

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
            datadog_tags=f"subscription:{company}",
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




    worker = Container(
        name=name,
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

    # I am fine with using print here
    # print("deleting resource")
    # poller = client.container_groups.begin_delete(
    #    resource_group_name=resource_group, container_group_name=worker.name)
    #
    # poller.wait()
    # while not poller.done():
    #     print("Waiting for delete")
    #     sleep(1)

    print("creating resource") # noqa: T201
    client.container_groups.begin_create_or_update(
        resource_group_name=resource_group,
        container_group_name=worker.name,
        container_group=group
    )

def deploy_all(tag: str, env: str) -> None:
    company_names = [
        "godtlevert",
        # "retnemt",
        # "adams",
        # "linas"
    ]

    bus_namespace = {
        "test": "gg-deviation-service-qa.servicebus.windows.net",
        "prod": "gg-deviation-service-prod.servicebus.windows.net"
    }

    for company in company_names:

        print(company) # noqa
        name = f"preselector-{company}-worker"
        if company == "godtlevert":
            name = "preselector-gl-worker"

        name += f"-{env}"
        print(name) # noqa

        deploy_preselector(
            name=name,
            company=company,
            service_bus_namespace=bus_namespace[env],
            env=env,
            image=f"bhregistry.azurecr.io/preselector:{tag}",
            resource_group=f"rg-chefdp-{env}",
        )


if __name__ == "__main__":
    deploy_all(tag="dev-latest", env="test")
