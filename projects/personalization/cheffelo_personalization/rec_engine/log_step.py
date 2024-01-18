import logging
import tracemalloc
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

logger = logging.getLogger(__name__)


@contextmanager
def log_step(step_name: str) -> Iterator[None]:
    if tracemalloc.is_tracing():
        now, max = tracemalloc.get_traced_memory()
        megabytes = 2**20
        logger.info(
            f"Starting with (current, max) bytes ({now / megabytes} MB, {max / megabytes} MB)"
        )

    logger.info(f"Starting to run {step_name}.")
    start_time = datetime.utcnow()
    did_fail = False

    try:
        yield
    except Exception:
        did_fail = True
        raise
    finally:
        end_time = datetime.utcnow() - start_time
        if did_fail:
            logger.info(f"Failed step: '{step_name}' in duration: {end_time}.")
        else:
            logger.info(f"Completed step: '{step_name}' in duration: {end_time}.")

        if tracemalloc.is_tracing():
            now, max = tracemalloc.get_traced_memory()
            megabytes = 2**20
            logger.info(
                f"Completed with (current, max) bytes ({now / megabytes} MB, {max / megabytes} MB)"
            )
