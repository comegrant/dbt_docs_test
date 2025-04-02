from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def duration(metric: str) -> Generator[None, None, None]:
    import os
    from time import monotonic

    from datadog.dogstatsd.base import statsd

    if statsd.host is None:
        yield
    else:
        metric_name = metric.lower().replace(" ", "-")
        metric_name = f"preselector.{metric_name}_time.histogram"
        start_time = monotonic()
        yield
        tags = None
        # Shit solution but but
        if "service_bus_request_topic_name" in os.environ:
            topic = os.environ["service_bus_request_topic_name".upper()]
            tags = [f"topic:{topic}"]
        statsd.histogram(metric_name, monotonic() - start_time, tags=tags)
