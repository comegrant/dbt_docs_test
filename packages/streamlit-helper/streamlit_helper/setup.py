import asyncio
import inspect
import logging
from contextlib import suppress
from pathlib import Path
from typing import Callable

from cheffelo_logging.logging import DataDogConfig, DataDogStatsdConfig, setup_datadog
from datadog import initialize
from datadog.dogstatsd.base import statsd
from pydantic import ValidationError


def setup_streamlit(  # noqa
    datadog_config: DataDogConfig | None = None, datadog_metrics_config: DataDogStatsdConfig | None = None
):
    if datadog_metrics_config is None:
        datadog_metrics_config = DataDogStatsdConfig()  # type: ignore

    def on_startup(func: Callable):  # noqa
        def lazy_run(*args, **kwargs):  # noqa
            initialize(statsd_host=datadog_metrics_config.datadog_host, statsd_port=datadog_metrics_config.datadog_port)

            wd = "/".join(Path.cwd().parts[-2:])
            statsd.increment(
                "sous-chef.streamlit.page-loads",
                tags=[f"function_name:{func.__name__}", f"module:{func.__module__}", f"working_dir:{wd}"],
            )

            logging.basicConfig(level=logging.INFO)
            logging.getLogger("azure").setLevel(logging.ERROR)

            with suppress(ValidationError):
                setup_datadog(
                    logger=logging.getLogger(""),
                    config=datadog_config or DataDogConfig(),  # type: ignore
                )

            if inspect.iscoroutinefunction(func):
                return asyncio.run(func(*args, **kwargs))
            else:
                return func(*args, **kwargs)

        return lazy_run

    return on_startup
