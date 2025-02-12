import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from time import sleep
from typing import Annotated, Literal

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
from keyvault import AzureKeyVault, DatabricksKeyVault, KeyVaultInterface
from preselector.process_stream_settings import ProcessStreamSettings
from pydantic import BaseModel, Field, SecretStr
from pydantic_argparser import parse_args
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class RunArgs(BaseModel):
    tag: str
    env: Literal["test", "prod"]
    mode: Literal["both", "batch", "live", "flush"] = Field("both")


class ScaleArgs(BaseModel):
    tag: str
    env: Literal["test", "prod"]
    worker_id: int
    company: Literal["godtlevert", "adams", "retnemt", "linas"]


class DeploySettings(BaseSettings):
    docker_registry_password: SecretStr
    user_assigned_identity_resource_id: str

    git_commit_hash: Annotated[str | None, Field] = None
    subscription_id: Annotated[str, Field] = "5a07602a-a1a5-43ee-9770-2cf18d1fdaf1"
    docker_registry_server: Annotated[str, Field] = "bhregistry.azurecr.io"
    docker_registry_username: Annotated[str, Field] = "bhregistry"


class RuntimeEnvs(BaseSettings):
    datalake_env: str
    uc_env: str

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
        resources=ResourceRequirements(requests=ResourceRequests(memory_in_gb=1.0, cpu=1.0, gpu=None)),
    )


@dataclass
class WorkerConfig:
    container_name: str
    batch_size: int
    topic_request_name: str
    topic_success_name: str
    sub_queue_name: str | None = field(default=None)


async def deploy_preselector(
    name: str,
    company: str,
    service_bus_namespace: str,
    env: str,
    image_tag: str,
    resource_group: str,
    workers: list[WorkerConfig],
    write_output_interval: timedelta | None = None,
) -> None:
    vault = key_vault()

    intervals = {
        "test": {
            "write_output_interval": write_output_interval or timedelta(hours=12),
            "write_output_max_size": 10_000,
            "update_data_interval": timedelta(hours=12),
        },
        "prod": {
            "write_output_interval": write_output_interval or timedelta(hours=3),
            "write_output_max_size": 10_000,
            "update_data_interval": timedelta(hours=2),
        },
    }

    deploy_settings = await vault.load(
        DeploySettings,
        key_map=lambda key: {"user_assigned_identity_resource_id": f"preselector-userAssignedIdentity-{env}"}.get(
            key, key.replace("_", "-")
        ),
    )
    client = ContainerInstanceManagementClient(
        credential=DefaultAzureCredential(), subscription_id=deploy_settings.subscription_id
    )
    dd_config = await vault.load(
        DataDogConfig,
        custom_values={
            "datadog_service_name": "preselector",
            "datadog_tags": f"subscription:{company},env:{env},image-tag:{image_tag}",
        },
    )

    if image_tag != "":
        image = f"bhregistry.azurecr.io/preselector:{image_tag.replace('/', '-')}"
    else:
        assert deploy_settings.git_commit_hash
        image = f"bhregistry.azurecr.io/preselector:{deploy_settings.git_commit_hash}"

    datadog_host = "datadog-agent"

    async def process_container(
        topic_request_name: str,
        topic_success_name: str,
        container_name: str,
        batch_size: int,
        sub_queue_name: str | None = None,
    ) -> Container:
        if env != "prod":
            assert env in deploy_settings.user_assigned_identity_resource_id

        environment_variables: list[EnvironmentVariable] = []

        env_specific_config = intervals.get(env, {})

        process_settings = await vault.load(
            ProcessStreamSettings,
            custom_values={
                "service_bus_subscription_name": company,
                "service_bus_namespace": service_bus_namespace,
                "service_bus_should_write": True,
                "service_bus_connection_string": None,
                "service_bus_request_topic_name": topic_request_name,
                "service_bus_request_size": batch_size,
                "service_bus_success_topic_name": topic_success_name,
                "service_bus_sub_queue": sub_queue_name,
                **env_specific_config,
            },
            key_map={
                "databricks_host": f"databricks-workspace-url-{env}",
                "databricks_token": f"databricks-sp-bundle-pat-{env}",
                "datalake_service_account_name": "azure-storageAccount-experimental-name",
                "datalake_storage_account_key": "azure-storageAccount-experimental-key",
            },
        )

        settings = [
            process_settings,
            DataDogStatsdConfig(
                # All containers are on localhost in the group
                datadog_host="127.0.0.1"
            ),
            DataDogConfig(  # type: ignore[reportGeneralTypeIssues]
                datadog_service_name="preselector",
                datadog_tags=dd_config.datadog_tags + f",topic:{topic_request_name},container:{container_name}",
            ),
            RuntimeEnvs(datalake_env=env, uc_env=env, env=env),
        ]

        for setting in settings:
            for key, value in setting.model_dump().items():
                if value is None:
                    continue

                if isinstance(value, SecretStr):
                    environment_variables.append(
                        EnvironmentVariable(name=key.upper(), secure_value=value.get_secret_value())
                    )
                else:
                    environment_variables.append(EnvironmentVariable(name=key.upper(), value=value))

        company_specific_memory = {"linas": 10, "godtlevert": 7, "retnemt": 4, "adams": 5}

        return Container(
            name=container_name,
            image=image,
            environment_variables=environment_variables,
            ports=[],
            command=["/bin/bash", "-c", "python -m preselector.process_stream"],
            resources=ResourceRequirements(
                requests=ResourceRequests(
                    # This is the max when having two workers and one logger container
                    # As the total sum is 16 GB
                    memory_in_gb=company_specific_memory.get(company, 6),
                    cpu=1.0,
                    gpu=None,
                )
            ),
        )

    worker_containers = await asyncio.gather(
        *[
            process_container(
                topic_success_name=worker.topic_success_name,
                topic_request_name=worker.topic_request_name,
                container_name=worker.container_name,
                batch_size=worker.batch_size,
                sub_queue_name=worker.sub_queue_name,
            )
            for worker in workers
        ]
    )
    group = ContainerGroup(
        containers=[datadog_agent_container(dd_config, name=datadog_host), *worker_containers],
        os_type=OperatingSystemTypes.LINUX,
        restart_policy=ContainerGroupRestartPolicy.ON_FAILURE,
        identity=ContainerGroupIdentity(
            type="UserAssigned",
            user_assigned_identities={deploy_settings.user_assigned_identity_resource_id: UserAssignedIdentities()},
        ),
        image_registry_credentials=[
            ImageRegistryCredential(
                server=deploy_settings.docker_registry_server,
                password=deploy_settings.docker_registry_password.get_secret_value(),
                username=deploy_settings.docker_registry_username,
            )
        ],
        location="northeurope",
        tags={
            "env": env,
            "company": company,
            "image_tag": image_tag,
            "resource_group": resource_group,
        },
    )

    logger.info("deleting resource")
    poller = client.container_groups.begin_delete(resource_group_name=resource_group, container_group_name=name)

    poller.wait()
    logger.info("Waiting for 10 secs")
    sleep(10)

    while not poller.done():
        logger.info("Waiting for delete")

    logger.info("creating resource")
    client.container_groups.begin_create_or_update(
        resource_group_name=resource_group, container_group_name=name, container_group=group
    )


async def deploy_all(tag: str, env: str, mode: Literal["both", "batch", "live", "flush"]) -> None:
    company_names = [
        "godtlevert",
        "adams",
        "linas",
        "retnemt",
    ]

    bus_namespace = {
        "test": "gg-deviation-service-qa.servicebus.windows.net",
        "prod": "gg-deviation-service-prod.servicebus.windows.net",
    }
    split_workers = ["linas"]

    for company in company_names:
        logger.info(company)
        name = f"preselector-{company}-worker"
        if company == "godtlevert":
            name = "preselector-gl-worker"

        if mode == "live":
            # Currently we want to have an additional scaler
            await scale_worker(tag=tag, env=env, worker_id=2, company=company, should_split=company in split_workers)
        elif mode == "both":
            name += f"-{env}"
            logger.info(name)
            workers = [
                WorkerConfig(
                    container_name=f"{name}-live",
                    batch_size=10,
                    topic_request_name="priority-deviation-request",
                    topic_success_name="priority-deviation-response",
                ),
                WorkerConfig(
                    container_name=f"{name}-batch",
                    batch_size=10,
                    topic_request_name="deviation-request",
                    topic_success_name="deviation-response",
                ),
            ]

            if company in split_workers:
                for index, worker in enumerate(workers):
                    await deploy_preselector(
                        name=name + f"-{index + 1}",
                        company=company,
                        service_bus_namespace=bus_namespace[env],
                        env=env,
                        image_tag=tag,
                        resource_group=f"rg-chefdp-{env}",
                        workers=[worker],
                    )
            else:
                await deploy_preselector(
                    name=name,
                    company=company,
                    service_bus_namespace=bus_namespace[env],
                    env=env,
                    image_tag=tag,
                    resource_group=f"rg-chefdp-{env}",
                    workers=workers,
                )
        elif mode == "flush":
            name += f"flush-{env}"
            logger.info(name)
            workers = [
                WorkerConfig(
                    container_name=f"{name}-flush",
                    batch_size=10,
                    topic_request_name="deviation-request",
                    topic_success_name="deviation-response",
                    sub_queue_name="deadletter",
                ),
            ]

            if company == "retnemt":
                await deploy_preselector(
                    name=name,
                    company=company,
                    service_bus_namespace=bus_namespace[env],
                    env=env,
                    image_tag=tag,
                    resource_group=f"rg-chefdp-{env}",
                    workers=workers,
                    write_output_interval=timedelta(seconds=1),
                )
        else:
            name += f"-batch-{env}"
            logger.info(name)

            await deploy_preselector(
                name=name,
                company=company,
                service_bus_namespace=bus_namespace[env],
                env=env,
                image_tag=tag,
                resource_group=f"rg-chefdp-{env}",
                workers=[
                    WorkerConfig(
                        container_name=f"{name}-batch-first",
                        batch_size=10,
                        topic_request_name="deviation-request",
                        topic_success_name="deviation-response",
                    ),
                    # WorkerConfig(
                    #     container_name=f"{name}-batch-second",
                    #     batch_size=10,
                    #     topic_name="deviation-request"
                    # )
                ],
            )


async def scale_worker(tag: str, env: str, worker_id: int, company: str, should_split: bool) -> None:
    logger.info(company)
    name = f"preselector-{company}-worker"
    if company == "godtlevert":
        name = "preselector-gl-worker"

    name += f"-{worker_id}-{env}"
    logger.info(name)

    bus_namespace = {
        "test": "gg-deviation-service-qa.servicebus.windows.net",
        "prod": "gg-deviation-service-prod.servicebus.windows.net",
    }
    workers = [
        WorkerConfig(
            container_name=f"{name}-live-first",
            batch_size=10,
            topic_request_name="priority-deviation-request",
            topic_success_name="priority-deviation-response",
        ),
        WorkerConfig(
            container_name=f"{name}-live-second",
            batch_size=10,
            topic_request_name="priority-deviation-request",
            topic_success_name="priority-deviation-response",
        ),
    ]

    if should_split:
        for index, worker in enumerate(workers):
            await deploy_preselector(
                name=name + f"-{index + 1}",
                company=company,
                service_bus_namespace=bus_namespace[env],
                env=env,
                image_tag=tag,
                resource_group=f"rg-chefdp-{env}",
                workers=[worker],
            )
    else:
        await deploy_preselector(
            name=name,
            company=company,
            service_bus_namespace=bus_namespace[env],
            env=env,
            image_tag=tag,
            resource_group=f"rg-chefdp-{env}",
            workers=workers,
        )


def key_vault() -> KeyVaultInterface:
    try:
        # Only works on the databricks runtime
        return DatabricksKeyVault(
            dbutils=dbutils,  # type: ignore
            scope="auth_common",
        )
    except NameError:
        return AzureKeyVault.from_vault_name("kv-chefdp-common")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("azure").setLevel(logging.ERROR)

    args = parse_args(RunArgs)
    await deploy_all(tag=args.tag, env=args.env, mode=args.mode)


if __name__ == "__main__":
    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main())
