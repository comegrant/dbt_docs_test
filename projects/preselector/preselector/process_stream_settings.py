from datetime import timedelta

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings


class ProcessStreamSettings(BaseSettings):
    datalake_service_account_name: str
    datalake_storage_account_key: SecretStr

    databricks_token: SecretStr
    databricks_host: str

    service_bus_connection_string: SecretStr | None = None
    service_bus_namespace: str | None = None

    service_bus_subscription_name: str | None = None
    service_bus_sub_queue: str | None = None

    service_bus_request_size: int = Field(10)
    service_bus_request_topic_name: str = Field("deviation-request")
    service_bus_success_topic_name: str = Field("deviation-response")
    service_bus_success_subscription_name: str = Field("data-team-log")
    service_bus_failed_topic_name: str = Field("deviation-error")
    service_bus_failed_subscription_name: str = Field("data-team-log")

    service_bus_should_write: bool = Field(True)

    update_data_interval: timedelta = Field(timedelta(hours=3))
    "The min interval between each data cache update"

    write_output_interval: timedelta | None = Field(None)
    "The min interval between each dump to our persistance storage. E.g. Databricks"

    write_output_max_size: int = Field(1000)

    ideal_poll_interval: float = Field(5)

    @property
    def is_batch_worker(self) -> bool:
        return self.service_bus_request_topic_name == "deviation-request"

    @property
    def environment(self) -> str:
        if not self.service_bus_namespace:
            return "unknown"

        if "prod" in self.service_bus_namespace:
            return "prod"
        elif "qa" in self.service_bus_namespace:
            return "test"
        else:
            return "unknown"
