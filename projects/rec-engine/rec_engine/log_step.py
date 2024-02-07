import logging
import tracemalloc
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@contextmanager
def log_step(step_name: str) -> Iterator[None]:
    if tracemalloc.is_tracing():
        now, max_memory = tracemalloc.get_traced_memory()
        megabytes = 2**20
        logger.info(
            f"Starting with (current, max) bytes ({now / megabytes} MB, {max_memory / megabytes} MB)",
        )

    logger.info(f"Starting to run {step_name}.")
    start_time = datetime.now(tz=timezone.utc)
    did_fail = False

    try:
        yield
    except Exception:
        did_fail = True
        raise
    finally:
        end_time = datetime.now(tz=timezone.utc) - start_time
        if did_fail:
            logger.info(f"Failed step: '{step_name}' in duration: {end_time}.")
        else:
            logger.info(f"Completed step: '{step_name}' in duration: {end_time}.")

        if tracemalloc.is_tracing():
            now, max_memory = tracemalloc.get_traced_memory()
            megabytes = 2**20
            logger.info(
                f"Completed with (current, max) bytes ({now / megabytes} MB, {max_memory / megabytes} MB)",
            )
