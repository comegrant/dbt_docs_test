from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings


class ProcessStreamSettings(BaseSettings):
    datalake_service_account_name: str
    datalake_storage_account_key: SecretStr

    service_bus_connection_string: SecretStr | None = None
    service_bus_namespace: str | None = None

    service_bus_subscription_name: str | None = None
    service_bus_sub_queue: str | None = None

    service_bus_request_topic_name: str = Field("deviation-request")
    service_bus_success_topic_name: str = Field("deviation-response")
    service_bus_failed_topic_name: str = Field("deviation-error")

    service_bus_should_write: bool = Field(True)

    ideal_poll_interval: float = Field(5)
