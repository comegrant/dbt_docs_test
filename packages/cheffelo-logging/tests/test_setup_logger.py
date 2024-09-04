import logging
import os

from cheffelo_logging import DataDogConfig, setup_datadog


def test_overwrite_config() -> None:

    logger = logging.getLogger(__name__)

    os.environ["DATADOG_API_KEY"] = "test"
    config = DataDogConfig(
        datadog_service_name="Test",
        datadog_tags="env:testing",
    ) # type: ignore

    assert len(logger.handlers) == 0

    setup_datadog(logger, config)

    assert len(logger.handlers) == 1
