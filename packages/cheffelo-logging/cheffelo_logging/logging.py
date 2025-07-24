import logging
from logging import Formatter, Logger, LogRecord, StreamHandler
from typing import Annotated, Optional

from datadog_api_client.v2 import ApiClient, ApiException, Configuration
from datadog_api_client.v2.api import logs_api
from datadog_api_client.v2.models import HTTPLogItem
from pydantic import Field
from pydantic_settings import BaseSettings
from pythonjsonlogger import jsonlogger


class DataDogStatsdConfig(BaseSettings):
    datadog_host: str = "127.0.0.1"
    datadog_port: Annotated[int, Field] = 8125


class DataDogSecrets(BaseSettings):
    datadog_api_key: str


class DataDogConfig(BaseSettings):
    datadog_api_key: str
    datadog_service_name: str
    datadog_tags: str

    datadog_site: Annotated[str, Field] = "datadoghq.eu"
    datadog_source: Annotated[str, Field] = "python"

    def configuration(self) -> Configuration:
        config = Configuration()
        config.api_key["apiKeyAuth"] = self.datadog_api_key
        config.server_variables["site"] = self.datadog_site
        return config


class DataDogStreamHandler(StreamHandler):
    "DataDog Handler used to push logs"

    def __init__(
        self,
        configuration: Configuration,
        service_name: str,
        source: str,
        tags: str,
    ):
        StreamHandler.__init__(self)
        self.configuration = configuration
        self.service_name = service_name
        self.source = source
        self.tags = tags

    def emit(self, record: LogRecord) -> None:
        msg = self.format(record)

        try:
            with ApiClient(self.configuration) as api_client:
                api_instance = logs_api.LogsApi(api_client)
                body = [
                    HTTPLogItem(
                        ddsource=self.source,
                        ddtags=self.tags,
                        message=msg,
                        service=self.service_name,
                        status=record.levelname,
                    )
                ]
                api_instance.submit_log(body)  # type: ignore
        except ApiException as api_exception:
            logging.exception("Exception when calling LogsApi->submit_log: %s\n", api_exception)


def setup_datadog(logger: Logger, config: DataDogConfig, formatter: Optional[Formatter] = None) -> None:
    """
    Setting up the datadog logger, given a config.

    ```python
    logger = logging.getLogger(__name__)

    # Variables can be set using env vars
    # Or by explicitly defining them in the init

    os.environ["DATADOG_API_KEY"] = "secret_key"

    config = DataDogConfig(
        datadog_service_name="preselector",
        datadog_tags="env:testing",
        datadog_app_key="secret_app_key"
    )
    setup_datadog(logger, config)

    logger.info("Hello DataDog!")
    ```
    """
    handler = DataDogStreamHandler(
        config.configuration(),
        service_name=config.datadog_service_name,
        source=config.datadog_source,
        tags=config.datadog_tags,
    )
    handler.setFormatter(
        formatter or jsonlogger.JsonFormatter("%(timestamp)s %(level)s %(name)s %(filename)s %(lineno)d %(message)s")
    )
    logger.addHandler(handler)


class StreamlitLogger(StreamHandler):
    "DataDog Handler used to push logs"

    def emit(self, record: LogRecord) -> None:
        import streamlit as st

        msg = self.format(record)

        if record.levelno <= logging.WARNING:
            st.warning(msg)
        elif record.levelno <= logging.INFO:
            st.info(msg)
        else:
            st.error(msg)


def setup_streamlit(logger: Logger, formatter: Optional[Formatter] = None) -> None:
    """
    Setting up the datadog logger, given a config.

    ```python
    logger = logging.getLogger(__name__)

    config = StreamlitLogger()
    setup_streamlit(logger)

    logger.info("Hello Streamlit!")
    ```
    """
    handler = StreamlitLogger()
    handler.setFormatter(
        formatter or jsonlogger.JsonFormatter("%(timestamp)s %(level)s %(name)s %(filename)s %(lineno)d %(message)s")
    )
    logger.addHandler(handler)
